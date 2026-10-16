package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ======================== TIỆN ÍCH ========================

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// ======================== TOR SUPERVISOR ========================

var torAvailable bool

func checkTorBinary() bool {
	if cfg.DisableTor || cfg.TorMax <= 0 {
		return false
	}
	if _, err := exec.LookPath(cfg.TorBin); err != nil {
		log.Printf("[Tor] không tìm thấy binary %q, chỉ dùng direct/proxy tĩnh", cfg.TorBin)
		return false
	}
	return true
}

// growTor bật thêm 1 Tor khi mọi đường ra đang bị giới hạn. Mặc định mỗi lần chỉ 1 instance đang khởi động.
func (m *Manager) growTor() { m.growTorN(1) }

// growTorN như growTor nhưng cho phép tối đa maxStarting instance cùng khởi động song song (bootstrap mất 10–45s nên
// khởi động nối tiếp sẽ làm đuôi độ trễ rất dài khi direct vừa cạn).
func (m *Manager) growTorN(maxStarting int) {
	if !torAvailable {
		return
	}
	m.mu.Lock()
	if m.torStarting >= maxStarting || m.torSpawned >= cfg.TorMax {
		m.mu.Unlock()
		return
	}
	m.torSpawned++
	m.torSeq++
	idx := m.torSeq
	m.torStarting++
	m.mu.Unlock()

	if err := m.startTor(idx); err != nil {
		log.Printf("[Tor] không bật được tor-%d: %v", idx, err)
		m.mu.Lock()
		m.torSpawned--
		m.torStarting--
		m.mu.Unlock()
	}
}

func (m *Manager) startTor(idx int) error {
	socks, err := freePort()
	if err != nil {
		return err
	}
	group := newTorGroup(fmt.Sprintf("tor-%d", idx), fmt.Sprintf("127.0.0.1:%d", socks), cfg.TorIdentities)
	e := group[0]

	dir := filepath.Join(os.TempDir(), fmt.Sprintf("tor-gw-%d", idx))
	e.dataDir = dir
	if err := os.MkdirAll(dir, 0o700); err != nil {
	}
	conf := filepath.Join(dir, "torrc")
	// MaxCircuitDirtiness dài: exit còn quota thì giữ lâu, chỉ đổi khi bị 429.
	torrc := fmt.Sprintf("SocksPort 127.0.0.1:%d IsolateSOCKSAuth\nDataDirectory %s\nLog notice stdout\nAvoidDiskWrites 1\nMaxCircuitDirtiness 7200\n",
		socks, filepath.Join(dir, "data"))
	// Phân tầng quốc gia 3 cấp theo chu kỳ daemon:
	// - idx % 3 == 1: Châu Á (ping thấp, ~1.5s): {sg},{jp},{hk},{kr}
	// - idx % 3 == 2: Tây Âu (bể IP gigabit khổng lồ, rất ít khi 429): {de},{nl},{fr},{gb},{ch}
	// - idx % 3 == 0: Toàn cầu (không giới hạn, nguồn IP dự phòng)
	switch idx % 3 {
	case 1:
		nodes := cfg.TorExitNodes
		if nodes == "" {
			nodes = "{sg},{jp},{hk},{kr}"
		}
		torrc += fmt.Sprintf("ExitNodes %s\n", nodes)
	case 2:
		eu := cfg.TorExitNodesEU
		if eu == "" {
			eu = "{de},{nl},{fr},{gb},{ch}"
		}
		torrc += fmt.Sprintf("ExitNodes %s\n", eu)
	case 0:
		// Toàn cầu: không đặt ExitNodes
	}
	if err := os.WriteFile(conf, []byte(torrc), 0o600); err != nil {
		return err
	}
	for _, x := range group {
		x.lastUsed.Store(time.Now().UnixNano())
		m.add(x)
	}
	ictx, cancel := context.WithCancel(m.ctx)
	e.stop = cancel
	e.lastUsed.Store(time.Now().UnixNano())
	m.wg.Add(1)
	go m.superviseTor(ictx, e, conf)
	return nil
}

// superviseTor chạy tor, đánh dấu ready khi bootstrap 100%, tự khởi động lại khi tor chết.
func (m *Manager) superviseTor(ctx context.Context, e *Egress, conf string) {
	first := true
	defer func() {
		m.wg.Done()
		if first {
			m.mu.Lock()
			m.torStarting--
			m.mu.Unlock()
		}
		if e.dataDir != "" {
			_ = os.RemoveAll(e.dataDir)
		}
	}()
	backoff := 2 * time.Second
	for ctx.Err() == nil {
		cmd := exec.CommandContext(ctx, cfg.TorBin, "-f", conf)
		cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
		cmd.WaitDelay = 5 * time.Second
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Printf("[%s] stdout pipe: %v", e.Name, err)
			return
		}
		cmd.Stderr = cmd.Stdout
		started := time.Now()
		if err := cmd.Start(); err != nil {
			log.Printf("[%s] start tor: %v", e.Name, err)
			if first {
				m.mu.Lock()
				m.torStarting--
				m.mu.Unlock()
				first = false
			}
		} else {
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				line := sc.Text()
				switch {
				case strings.Contains(line, "Bootstrapped 100%"):
					e.setGroupReady(true)
					go warmIdentities(ctx, e.members)
					log.Printf("[%s] Tor bootstrapped, sẵn sàng (%.0fs)", e.Name, time.Since(started).Seconds())
					if first {
						m.mu.Lock()
						m.torStarting--
						m.mu.Unlock()
						first = false
					}
				case strings.Contains(line, "[warn]") || strings.Contains(line, "[err]"):
					log.Printf("[%s] %s", e.Name, line)
				}
			}
			_ = cmd.Wait()
		}
		if first {
			m.mu.Lock()
			m.torStarting--
			m.mu.Unlock()
			first = false
		}
		e.setGroupReady(false)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			backoff = 2 * time.Second
		}
		log.Printf("[%s] Tor dừng, khởi động lại sau %v", e.Name, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
}

// warmTor bật sẵn TOR_WARM instance để khi direct bị 429 là chuyển được ngay.
func (m *Manager) warmTor() {
	for i := 0; i < cfg.TorWarm; i++ {
		m.growTorN(cfg.TorWarm)
		time.Sleep(300 * time.Millisecond)
	}
}

// warmIdentities dựng sẵn circuit + kết nối TLS/HTTP2 cho từng identity (dãn cách nhẹ để không dồn Tor), để request đầu
// tiên của client không phải chờ 1–15 giây dựng circuit. Chỉ HEAD trang chủ, không đụng API nên không tốn hạn mức.
func warmIdentities(ctx context.Context, ids []*Egress) {
	var wg sync.WaitGroup
	for i, e := range ids {
		wg.Add(1)
		go func(e *Egress, delay time.Duration) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			c, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if req, err := http.NewRequestWithContext(c, http.MethodHead, "https://opencode.ai/", nil); err == nil {
				if resp, err := e.Client().Do(req); err == nil {
					resp.Body.Close()
				}
			}
		}(e, time.Duration(i)*150*time.Millisecond)
	}
	wg.Wait()
}

// autoscaleTor bật thêm daemon Tor khi tải đồng thời trên mỗi daemon vượt TOR_LOAD (Tor đơn luồng: 1 daemon chịu ~25–30
// request đồng thời thì ttfb bắt đầu tăng rõ). Tắt bớt do reapIdleTor khi nhàn rỗi.
func (m *Manager) autoscaleTor() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
		ready, inflight := 0, 0
		for _, e := range m.snapshot() {
			if e.Kind != kindTor {
				continue
			}
			inflight += int(e.inflight.Load())
			if len(e.members) > 0 && e.ready.Load() {
				ready++
			}
		}
		if inflight > 0 && inflight > ready*cfg.TorLoad {
			m.growTorN(2)
		}
	}
}
