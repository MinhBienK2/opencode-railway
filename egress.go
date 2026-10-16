package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
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
	stop     context.CancelFunc // dừng Tor do gateway tự chạy (chỉ ở leader)
	latMu    sync.Mutex
	latEwma  time.Duration // trung bình trượt ttfb của circuit hiện tại
	latN     int
	latGen   uint64
	members  []*Egress // leader của 1 daemon Tor: toàn bộ identity (gồm cả chính nó)
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

// newTorGroup tạo k "identity" trên cùng 1 daemon Tor. Mỗi identity có username SOCKS riêng nên có circuit/exit IP riêng,
// tức hạn mức upstream riêng (quota tính theo IP): dung lượng tăng theo số identity chứ không chỉ theo số daemon.
func newTorGroup(name, socksAddr string, k int) []*Egress {
	if k < 1 {
		k = 1
	}
	g := make([]*Egress, k)
	for i := range g {
		g[i] = newTorEgress(fmt.Sprintf("%s.%d", name, i+1), socksAddr)
	}
	g[0].members = g
	return g
}

// setGroupReady đặt trạng thái sẵn sàng cho toàn bộ identity của daemon.
func (e *Egress) setGroupReady(v bool) {
	if len(e.members) == 0 {
		e.ready.Store(v)
		return
	}
	for _, x := range e.members {
		x.ready.Store(v)
	}
}

// groupLastUsed: lần dùng gần nhất của bất kỳ identity nào trong daemon.
func (e *Egress) groupLastUsed() int64 {
	last := e.lastUsed.Load()
	for _, x := range e.members {
		if v := x.lastUsed.Load(); v > last {
			last = v
		}
	}
	return last
}

// groupInflight: số request đang chạy trên cả daemon.
func (e *Egress) groupInflight() int {
	if len(e.members) == 0 {
		return int(e.inflight.Load())
	}
	n := 0
	for _, x := range e.members {
		n += int(x.inflight.Load())
	}
	return n
}

// observeLatency ghi ttfb của 1 request thành công (gen = circuit đã dùng; circuit mới thì bắt đầu lại từ đầu).
func (e *Egress) observeLatency(gen uint64, ttfb time.Duration) {
	e.latMu.Lock()
	defer e.latMu.Unlock()
	if gen != e.latGen {
		e.latGen, e.latEwma, e.latN = gen, 0, 0
	}
	if e.latN == 0 {
		e.latEwma = ttfb
	} else {
		e.latEwma = (e.latEwma*7 + ttfb*3) / 10
	}
	e.latN++
}

// latency trả về (ttfb trung bình trượt, số mẫu) của circuit hiện tại.
func (e *Egress) latency() (time.Duration, int) {
	e.latMu.Lock()
	defer e.latMu.Unlock()
	if e.latGen != e.gen.Load() {
		return 0, 0
	}
	return e.latEwma, e.latN
}

// medianLatency: trung vị ttfb của các egress Tor/proxy đã đủ mẫu (0 nếu chưa đủ 3 egress để so sánh).
func (m *Manager) medianLatency() time.Duration {
	var l []time.Duration
	for _, e := range m.snapshot() {
		if e.Kind == kindDirect {
			continue
		}
		if d, n := e.latency(); n >= 3 {
			l = append(l, d)
		}
	}
	if len(l) < 3 {
		return 0
	}
	sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	return l[len(l)/2]
}

// retireIfSlow: circuit chậm hơn hẳn các circuit khác (exit tồi) thì đổi sang exit mới; dần dần chỉ còn lại exit nhanh.
func (m *Manager) retireIfSlow(e *Egress, gen uint64) {
	if !e.canRotate {
		return
	}
	d, n := e.latency()
	if n < 6 {
		return
	}
	med := m.medianLatency()
	if med > 0 && d > 3*time.Second && d > med*5/2 {
		log.Printf("[%s] circuit chậm (ttfb≈%v, trung vị %v), đổi exit", e.Name, d.Round(time.Millisecond), med.Round(time.Millisecond))
		e.rotate(gen)
	}
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

	torSpawned  int
	torSeq      int
	torStarting int
	ctx         context.Context
	wg          sync.WaitGroup
}

func newManager(ctx context.Context) *Manager {
	m := &Manager{ctx: ctx, dirWin: map[string]*rateWindow{}}
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
		for _, e := range newTorGroup(fmt.Sprintf("tor-ext%d", i+1), parts[0]+":"+parts[1], cfg.TorIdentities) {
			e.ready.Store(true)
			m.add(e)
		}
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

// pick chọn egress cho model (cache của upstream theo prefix chứ không theo IP/session nên không cần gắn hội thoại với egress): mặc định Tor/proxy trước (PREFER_DIRECT=true thì direct trước); hết thì ưu tiên egress Tor đã phục vụ hội thoại này (giữ cache) nếu nó còn khỏe,
// sau đó Tor/proxy khỏe nhất/rảnh nhất. nil nếu chưa có cái nào dùng được.
func (m *Manager) pick(model string) *Egress {
	list := m.snapshot()
	direct := list[0]
	if cfg.PreferDirect && direct.available(model) && direct.claim(model) {
		return direct
	}
	if e := m.pickRest(list[1:], model); e != nil {
		return e
	}
	// Tor/proxy chưa sẵn sàng hoặc đều đang nghỉ: direct làm đường dự phòng thay vì bắt client chờ.
	if !cfg.PreferDirect && direct.available(model) && direct.claim(model) {
		return direct
	}
	return nil
}

// loadScore: càng thấp càng nên chọn. Circuit chưa đủ mẫu coi như bằng trung vị (để được thử), mỗi request đang chạy
// cộng thêm ~25% chi phí để tải dàn đều chứ không dồn vào circuit nhanh nhất.
func (e *Egress) loadScore(median time.Duration) float64 {
	d, n := e.latency()
	if n < 3 {
		d = median
		if d == 0 {
			d = time.Second
		}
	}
	return float64(d) * (1 + 0.25*float64(e.inflight.Load()))
}

func (m *Manager) pickRest(rest []*Egress, model string) *Egress {
	if len(rest) == 0 {
		return nil
	}
	start := int(atomic.AddUint32(&m.rr, 1))
	// Ưu tiên egress đang khỏe (ít strike nhất), sau đó ít inflight nhất; hòa thì xoay vòng.
	var best *Egress
	var bestStrikes int
	var bestScore float64
	med := m.medianLatency()
	for i := range rest {
		e := rest[(start+i)%len(rest)]
		if !e.available(model) || (e.canRotate && int(e.inflight.Load()) >= cfg.TorMaxInflight) {
			continue
		}
		st := e.strikes(model)
		sc := e.loadScore(med)
		if best == nil || st < bestStrikes || (st == bestStrikes && sc < bestScore) {
			best, bestStrikes, bestScore = e, st, sc
		}
	}
	if best != nil && !best.claim(model) {
		return nil // vừa bị request khác giữ chỗ thăm dò; route sẽ thử lại
	}
	return best
}

// route chọn (model, egress). Không có egress nào dùng được thì bật thêm Tor và chờ tới deadline.
func (m *Manager) route(ctx context.Context, model string, isPool bool, deadline time.Time) (string, *Egress, *gwError) {
	for {
		if isPool {
			n := len(freeModelPool)
			start := int(atomic.AddUint32(&poolIndex, 1))
			for i := 0; i < n; i++ {
				cand := freeModelPool[(start+i)%n]
				if e := m.pick(cand); e != nil {
					return cand, e, nil
				}
			}
		} else if e := m.pick(model); e != nil {
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
			if e.stop == nil || e.groupInflight() > 0 || !e.ready.Load() {
				continue
			}
			if idle := time.Since(time.Unix(0, e.groupLastUsed())); idle > cfg.TorIdleStop {
				log.Printf("[%s] nhàn rỗi %v, tắt để nhường RAM", e.Name, idle.Round(time.Second))
				m.removeTor(e)
			}
		}
	}
}

func (m *Manager) removeTor(e *Egress) {
	e.setGroupReady(false)
	gone := map[*Egress]bool{e: true}
	for _, x := range e.members {
		gone[x] = true
	}
	m.mu.Lock()
	var keep []*Egress
	for _, x := range m.egresses {
		if !gone[x] {
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
