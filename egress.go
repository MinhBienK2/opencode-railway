package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ======================== EGRESS ========================
// Egress = 1 đường ra Internet: IP trực tiếp của container, hoặc 1 SOCKS5/HTTP proxy (Tor...).
// Quota của upstream tính theo (IP × model) nên cooldown cũng lưu theo (egress × model):
// direct hết quota deepseek không ảnh hưởng direct dùng cho model khác.

const (
	kindDirect = "direct"
	kindTor    = "tor"
	kindProxy  = "proxy"
)

type coolState struct {
	until   time.Time
	strikes int
}

type Egress struct {
	Name      string
	Kind      string
	canRotate bool // Tor: đổi được exit bằng cách đổi thông tin xác thực SOCKS (IsolateSOCKSAuth)

	client atomic.Pointer[http.Client]
	ready  atomic.Bool
	gen    atomic.Uint64 // "thế hệ" circuit; tăng lên là chuyển sang circuit/exit mới

	inflight atomic.Int32
	lastUsed atomic.Int64       // unix nano của lần dùng gần nhất (để tắt Tor nhàn rỗi)
	stop     context.CancelFunc // dừng Tor do gateway tự chạy
	reqs     atomic.Int64
	oks      atomic.Int64
	r429     atomic.Int64
	errs     atomic.Int64

	mu   sync.Mutex
	cool map[string]*coolState
}

func newTransport(proxy func(*http.Request) (*url.URL, error)) *http.Transport {
	return &http.Transport{
		Proxy: proxy,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   512,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   20 * time.Second,
		ResponseHeaderTimeout: 120 * time.Second, // không đặt Client.Timeout để stream dài không bị cắt
		ExpectContinueTimeout: 1 * time.Second,
	}
}

func newEgress(name, kind string, proxy func(*http.Request) (*url.URL, error)) *Egress {
	e := &Egress{Name: name, Kind: kind, cool: make(map[string]*coolState)}
	e.client.Store(&http.Client{Transport: newTransport(proxy)})
	return e
}

// newTorEgress: SOCKS5 tới 1 Tor. Mỗi "thế hệ" dùng username khác nhau => Tor tách circuit riêng (IsolateSOCKSAuth).
// Transport của Go tách pool kết nối theo URL proxy nên kết nối mới đi circuit mới, không bị HTTP/2 dồn vào kết nối cũ.
func newTorEgress(name, socksAddr string) *Egress {
	var e *Egress
	nonce := randomHex(6)
	proxy := func(*http.Request) (*url.URL, error) {
		return &url.URL{Scheme: "socks5", Host: socksAddr,
			User: url.UserPassword(fmt.Sprintf("g%d-%s", e.gen.Load(), nonce), "x")}, nil
	}
	e = newEgress(name, kindTor, proxy)
	e.canRotate = true
	return e
}

func (e *Egress) Client() *http.Client { return e.client.Load() }

func (e *Egress) available(model string) bool {
	if !e.ready.Load() {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	c, ok := e.cool[model]
	return !ok || !time.Now().Before(c.until)
}

// strikes: số lần 429 liên tiếp của (egress, model) kể từ lần thành công gần nhất (0 = khỏe/chưa thử).
func (e *Egress) strikes(model string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c, ok := e.cool[model]; ok {
		return c.strikes
	}
	return 0
}

// claim giữ chỗ để dùng egress cho model. Egress đã từng bị 429 (còn strike) mà hết cooldown chỉ cho 1 request
// "thăm dò" tại một thời điểm: giữ chỗ bằng cách đẩy until ra probeLease; kết quả thăm dò sẽ ghi đè
// (429 => cooldown thật, thành công => clear). Egress khỏe thì không giới hạn.
func (e *Egress) claim(model string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, ok := e.cool[model]
	if !ok {
		return true
	}
	if time.Now().Before(c.until) {
		return false
	}
	if c.strikes > 0 {
		c.until = time.Now().Add(probeLease)
	}
	return true
}

const probeLease = 15 * time.Second

func (e *Egress) clear(model string) {
	e.mu.Lock()
	delete(e.cool, model)
	e.mu.Unlock()
}

// transient: lỗi tạm thời (502, lỗi mạng), nghỉ ngắn, không tăng strike.
func (e *Egress) transient(model string, d time.Duration) {
	e.mu.Lock()
	c := e.cool[model]
	if c == nil {
		c = &coolState{}
		e.cool[model] = c
	}
	if until := time.Now().Add(d); until.After(c.until) {
		c.until = until
	}
	e.mu.Unlock()
}

// exhausted: (fresh = request này đi trên circuit hiện tại, chưa bị request khác xoay) upstream trả 429 cho (egress, model). Trả về thời gian nghỉ đã áp dụng.
//   - Tor có control port: IP sẽ được đổi nên chỉ nghỉ ngắn rồi thử lại với exit mới.
//   - Còn lại (direct, proxy tĩnh): theo Retry-After (tối đa CooldownMax) nếu có, không thì backoff mũ 15s, 30s, 1m... tới CooldownMax.
//     Mỗi lần hết hạn chỉ có 1 request "thăm dò" lại; thành công thì clear().
func (e *Egress) exhausted(model string, retryAfter time.Duration, fresh bool) time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.cool[model]
	if c == nil {
		c = &coolState{}
		e.cool[model] = c
	}
	// Nhiều request đồng thời cùng nhận 429 của 1 sự kiện chỉ tính là 1 lần: nếu đang cooldown (direct) hoặc exit đã được
	// đổi bởi request khác (Tor, fresh=false) thì không cộng strike, không kéo dài backoff.
	if now := time.Now(); (!e.canRotate && now.Before(c.until)) || (e.canRotate && !fresh) {
		if left := c.until.Sub(now); left > 0 {
			return left
		}
		return cfg.TorRotateCooldown
	}
	c.strikes++
	var d time.Duration
	switch {
	case e.canRotate:
		// Đổi circuit là có exit mới nên thử lại gần như ngay; nhưng nếu 5+ lần liên tiếp đều 429 thì có thể cả bể exit
		// hôm nay đã cạn: giãn dần để không đập vào upstream.
		d = cfg.TorRotateCooldown
		if c.strikes > 4 {
			shift := c.strikes - 4
			if shift > 10 {
				shift = 10
			}
			d = cfg.TorRotateCooldown << shift
			if d > cfg.TorBackoffMax {
				d = cfg.TorBackoffMax
			}
		}
	case retryAfter > 0 && retryAfter <= cfg.CooldownMax:
		d = retryAfter
		if d > cfg.CooldownMax {
			d = cfg.CooldownMax
		}
	default:
		// Retry-After dài (vd 22h) không đáng tin: đo thực tế IP direct hồi phục sau ~1–2 phút (cửa sổ trượt), nên thăm dò
		// sớm bằng 1 request duy nhất (probe lease) và giãn dần nếu vẫn 429.
		shift := c.strikes - 1
		if shift > 12 {
			shift = 12
		}
		d = 15 * time.Second << shift
		if d > cfg.CooldownMax {
			d = cfg.CooldownMax
		}
	}
	c.until = time.Now().Add(d)
	return d
}

func (e *Egress) cooldownLeft(model string) time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c, ok := e.cool[model]; ok {
		if d := time.Until(c.until); d > 0 {
			return d
		}
	}
	return 0
}

// rotate chuyển Tor sang circuit/exit mới cho các kết nối mới (không giới hạn 10s như NEWNYM).
// Request đang chạy dở vẫn giữ circuit cũ cho tới khi xong. seen là thế hệ mà request đã dùng: nếu
// request khác đã xoay rồi (gen đã khác) thì không xoay chồng thêm.
func (e *Egress) rotate(seen uint64) {
	if !e.canRotate {
		return
	}
	if e.gen.CompareAndSwap(seen, seen+1) {
		log.Printf("[%s] đổi circuit (gen=%d)", e.Name, seen+1)
	}
}

// ======================== MANAGER ========================

type Manager struct {
	mu       sync.RWMutex
	egresses []*Egress // egresses[0] luôn là direct (có thể bị vô hiệu bởi FORCE_TOR)
	rr       uint32

	dirMu  sync.Mutex
	dirWin map[string]*rateWindow

	stickyMu sync.Mutex
	sticky   map[string]*Egress // khoá hội thoại -> egress Tor đã phục vụ lần trước (giữ cache prefix)

	torSpawned  int
	torSeq      int
	torStarting int
	ctx         context.Context
	wg          sync.WaitGroup
}

func newManager(ctx context.Context) *Manager {
	m := &Manager{ctx: ctx, sticky: map[string]*Egress{}, dirWin: map[string]*rateWindow{}}
	direct := newEgress("direct", kindDirect, http.ProxyFromEnvironment)
	direct.ready.Store(!cfg.ForceTor)
	m.egresses = []*Egress{direct}
	return m
}

// warmDirect giữ sẵn kết nối TLS/HTTP2 tới upstream để request đầu tiên không phải trả phí bắt tay (không đụng API nên không tốn hạn mức).
func (m *Manager) warmDirect() {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		if d := m.egresses[0]; d.ready.Load() {
			ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
			if req, err := http.NewRequestWithContext(ctx, http.MethodHead, "https://opencode.ai/", nil); err == nil {
				if resp, err := d.Client().Do(req); err == nil {
					resp.Body.Close()
				}
			}
			cancel()
		}
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (m *Manager) snapshot() []*Egress {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.egresses
}

func (m *Manager) add(e *Egress) {
	m.mu.Lock()
	m.egresses = append(m.egresses, e)
	m.mu.Unlock()
}

// addStatic nạp các proxy khai báo sẵn: TOR_INSTANCES (host:socks, phần :control nếu có sẽ bị bỏ qua) và PROXY_POOL (URL).
func (m *Manager) addStatic(torInstances, proxyPool string) {
	if cfg.DisableTor {
		return
	}
	for i, item := range splitList(torInstances) {
		parts := strings.Split(item, ":")
		if len(parts) < 2 {
			continue
		}
		e := newTorEgress(fmt.Sprintf("tor-ext%d", i+1), parts[0]+":"+parts[1])
		e.ready.Store(true)
		m.add(e)
	}
	for i, p := range splitList(proxyPool) {
		u, err := url.Parse(p)
		if err != nil {
			continue
		}
		e := newEgress(fmt.Sprintf("proxy%d", i+1), kindProxy, http.ProxyURL(u))
		e.ready.Store(true)
		m.add(e)
	}
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (m *Manager) stickyGet(key string) *Egress {
	if key == "" {
		return nil
	}
	m.stickyMu.Lock()
	defer m.stickyMu.Unlock()
	return m.sticky[key]
}

func (m *Manager) stickySet(key string, e *Egress) {
	if key == "" || e == nil || e.Kind == kindDirect {
		return
	}
	m.stickyMu.Lock()
	if len(m.sticky) >= 4096 {
		m.sticky = map[string]*Egress{}
	}
	m.sticky[key] = e
	m.stickyMu.Unlock()
}

// pick chọn egress cho model: direct trước; hết thì ưu tiên egress Tor đã phục vụ hội thoại này (giữ cache) nếu nó còn khỏe,
// sau đó Tor/proxy khỏe nhất/rảnh nhất. nil nếu chưa có cái nào dùng được.
func (m *Manager) pick(model string, prefer *Egress) *Egress {
	list := m.snapshot()
	if d := list[0]; d.available(model) && d.claim(model) {
		return d
	}
	rest := list[1:]
	if len(rest) == 0 {
		return nil
	}
	if prefer != nil && prefer.Kind != kindDirect && prefer.available(model) && prefer.strikes(model) == 0 &&
		int(prefer.inflight.Load()) < cfg.TorMaxInflight && prefer.claim(model) {
		return prefer
	}
	start := int(atomic.AddUint32(&m.rr, 1))
	// Ưu tiên egress đang khỏe (ít strike nhất), sau đó ít inflight nhất; hòa thì xoay vòng.
	var best *Egress
	var bestStrikes int
	for i := range rest {
		e := rest[(start+i)%len(rest)]
		if !e.available(model) || (e.canRotate && int(e.inflight.Load()) >= cfg.TorMaxInflight) {
			continue
		}
		st := e.strikes(model)
		if best == nil || st < bestStrikes || (st == bestStrikes && e.inflight.Load() < best.inflight.Load()) {
			best, bestStrikes = e, st
		}
	}
	if best != nil && !best.claim(model) {
		return nil // vừa bị request khác giữ chỗ thăm dò; route sẽ thử lại
	}
	return best
}

// route chọn (model, egress). Không có egress nào dùng được thì bật thêm Tor và chờ tới deadline.
func (m *Manager) route(ctx context.Context, model string, isPool bool, deadline time.Time, prefer *Egress) (string, *Egress, *gwError) {
	for {
		if isPool {
			n := len(freeModelPool)
			start := int(atomic.AddUint32(&poolIndex, 1))
			for i := 0; i < n; i++ {
				cand := freeModelPool[(start+i)%n]
				if e := m.pick(cand, prefer); e != nil {
					return cand, e, nil
				}
			}
		} else if e := m.pick(model, prefer); e != nil {
			return model, e, nil
		}

		m.growTor()

		models := []string{model}
		if isPool {
			models = freeModelPool
		}
		// Không có Tor nào đang/sắp chạy và đường nhanh nhất còn khoá lâu hơn thời gian chờ cho phép: báo ngay, đừng treo client.
		if !m.canWaitForTor() {
			if sooner := m.soonest(models); sooner == 0 || sooner > time.Until(deadline) {
				return "", nil, &gwError{
					status:     http.StatusTooManyRequests,
					typ:        "rate_limit_error",
					msg:        "All egress paths are rate limited for this model. Retry later.",
					retryAfter: sooner,
				}
			}
		}

		if time.Now().After(deadline) {
			return "", nil, &gwError{
				status:     http.StatusTooManyRequests,
				typ:        "rate_limit_error",
				msg:        "All egress paths are rate limited for this model. Retry later.",
				retryAfter: m.soonest(models),
			}
		}
		select {
		case <-ctx.Done():
			return "", nil, &gwError{status: 499, typ: "client_closed", msg: "client closed request"}
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// soonest: thời gian ngắn nhất cho tới khi có 1 egress dùng lại được cho 1 trong các model.
func (m *Manager) soonest(models []string) time.Duration {
	best := time.Duration(0)
	for _, e := range m.snapshot() {
		if !e.ready.Load() {
			continue
		}
		for _, mod := range models {
			if d := e.cooldownLeft(mod); d > 0 && (best == 0 || d < best) {
				best = d
			}
		}
	}
	return best
}

// canWaitForTor: còn Tor đang khởi động, hoặc còn chỗ để bật thêm Tor.
func (m *Manager) canWaitForTor() bool {
	if !torAvailable {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.torStarting > 0 || m.torSpawned < cfg.TorMax
}

// ---------- Phát hiện "nguy hiểm" ở direct để bật Tor sẵn ----------

// rateWindow đếm số sự kiện trong 120 giây gần nhất (12 ô × 10 giây).
type rateWindow struct {
	buckets [12]int
	last    int64
}

func (w *rateWindow) add(nowSec int64) int {
	idx := nowSec / 10
	if steps := idx - w.last; steps > 0 {
		if steps >= 12 {
			w.buckets = [12]int{}
		} else {
			for i := int64(1); i <= steps; i++ {
				w.buckets[(w.last+i)%12] = 0
			}
		}
		w.last = idx
	}
	w.buckets[idx%12]++
	sum := 0
	for _, b := range w.buckets {
		sum += b
	}
	return sum
}

// noteDirect ghi nhận 1 request direct cho model và bật Tor sẵn nếu lưu lượng/đồng thời tiến gần ngưỡng nguy hiểm.
func (m *Manager) noteDirect(model string, directInflight int) {
	m.dirMu.Lock()
	w := m.dirWin[model]
	if w == nil {
		w = &rateWindow{}
		m.dirWin[model] = w
	}
	n := w.add(time.Now().Unix())
	m.dirMu.Unlock()
	if n >= cfg.DirectWarnReqs || directInflight >= cfg.DirectWarnInflight {
		m.prewarmTor("lưu lượng direct gần ngưỡng", 1)
	}
}

// prewarmTor đảm bảo có sẵn ít nhất `want` Tor đang chạy/khởi động (song song), chạy nền. Direct vừa 429 thì dựng 2 Tor
// cùng lúc để có chỗ chịu tải ngay khi bootstrap xong.
func (m *Manager) prewarmTor(reason string, want int) {
	if !torAvailable {
		return
	}
	if want > cfg.TorMax {
		want = cfg.TorMax
	}
	m.mu.RLock()
	have := m.torSpawned
	static := len(m.egresses) > 1 && m.torSpawned == 0
	m.mu.RUnlock()
	if static || have >= want {
		return
	}
	log.Printf("[Tor] bật %d Tor dự phòng (%s)", want-have, reason)
	for i := have; i < want; i++ {
		go m.growTorN(want)
	}
}

// reapIdleTor tắt Tor do gateway chạy mà không có request quá TorIdleStop.
func (m *Manager) reapIdleTor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
		for _, e := range m.snapshot() {
			if e.stop == nil || e.inflight.Load() > 0 || !e.ready.Load() {
				continue
			}
			if idle := time.Since(time.Unix(0, e.lastUsed.Load())); idle > cfg.TorIdleStop {
				log.Printf("[%s] nhàn rỗi %v, tắt để nhường RAM", e.Name, idle.Round(time.Second))
				m.removeTor(e)
			}
		}
	}
}

func (m *Manager) removeTor(e *Egress) {
	e.ready.Store(false)
	m.mu.Lock()
	var keep []*Egress
	for _, x := range m.egresses {
		if x != e {
			keep = append(keep, x)
		}
	}
	m.egresses = keep
	if m.torSpawned > 0 {
		m.torSpawned--
	}
	m.mu.Unlock()
	if e.stop != nil {
		e.stop()
	}
}
