package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// growTor bật thêm 1 Tor khi mọi đường ra đang bị giới hạn. Mỗi lần chỉ 1 instance đang khởi động.
func (m *Manager) growTor() {
	if !torAvailable {
		return
	}
	m.mu.Lock()
	if m.torStarting > 0 || m.torSpawned >= cfg.TorMax {
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
	e := newTorEgress(fmt.Sprintf("tor-%d", idx), fmt.Sprintf("127.0.0.1:%d", socks))

	dir := filepath.Join(os.TempDir(), fmt.Sprintf("tor-gw-%d", idx))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	conf := filepath.Join(dir, "torrc")
	// MaxCircuitDirtiness dài: exit còn quota thì giữ lâu, chỉ đổi khi bị 429.
	torrc := fmt.Sprintf("SocksPort 127.0.0.1:%d IsolateSOCKSAuth\nDataDirectory %s\nLog notice stdout\nAvoidDiskWrites 1\nMaxCircuitDirtiness 7200\n",
		socks, filepath.Join(dir, "data"))
	if err := os.WriteFile(conf, []byte(torrc), 0o600); err != nil {
		return err
	}
	m.add(e)
	ictx, cancel := context.WithCancel(m.ctx)
	e.stop = cancel
	e.lastUsed.Store(time.Now().UnixNano())
	m.wg.Add(1)
	go m.superviseTor(ictx, e, conf)
	return nil
}

// superviseTor chạy tor, đánh dấu ready khi bootstrap 100%, tự khởi động lại khi tor chết.
func (m *Manager) superviseTor(ctx context.Context, e *Egress, conf string) {
	defer m.wg.Done()
	backoff := 2 * time.Second
	first := true
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
					e.ready.Store(true)
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
		e.ready.Store(false)
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
		// startTor tuần tự: mỗi lần chờ instance trước bootstrap xong để không tranh băng thông khởi động.
		m.growTor()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) && m.ctx.Err() == nil {
			m.mu.RLock()
			starting := m.torStarting
			m.mu.RUnlock()
			if starting == 0 {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}
