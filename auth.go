package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// ======================== XÁC THỰC THEO IP ========================

// ipAllowlist: rỗng => chỉ loopback + mạng nội bộ (an toàn mặc định); "*" => mở hoàn toàn.
type ipAllowlist struct {
	all      bool
	prefixes []netip.Prefix
}

func parseAllowlist(raw string) (*ipAllowlist, error) {
	al := &ipAllowlist{}
	for _, item := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' || r == ';' }) {
		if item == "*" {
			al.all = true
			continue
		}
		if strings.Contains(item, "/") {
			p, err := netip.ParsePrefix(item)
			if err != nil {
				return nil, fmt.Errorf("ALLOWED_IPS: %q: %w", item, err)
			}
			al.prefixes = append(al.prefixes, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(item)
		if err != nil {
			return nil, fmt.Errorf("ALLOWED_IPS: %q: %w", item, err)
		}
		a = a.Unmap()
		al.prefixes = append(al.prefixes, netip.PrefixFrom(a, a.BitLen()))
	}
	return al, nil
}

func (al *ipAllowlist) empty() bool { return !al.all && len(al.prefixes) == 0 }

// cgnat 100.64.0.0/10: dải của Tailscale/CGNAT, coi như mạng riêng.
var cgnat = netip.MustParsePrefix("100.64.0.0/10")

func (al *ipAllowlist) allows(ip netip.Addr) bool {
	if al.all {
		return true
	}
	if al.empty() {
		return ip.IsLoopback() || ip.IsPrivate() || cgnat.Contains(ip.Unmap())
	}
	for _, p := range al.prefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// clientIP lấy IP client. Nếu header được cấu hình (CLIENT_IP_HEADER) thì tin header đó,
// với X-Forwarded-For lấy phần tử ngoài cùng bên phải (do proxy tin cậy gắn vào, không giả mạo được).
func clientIP(r *http.Request, header string) (netip.Addr, bool) {
	if header != "" {
		if v := strings.TrimSpace(r.Header.Get(header)); v != "" {
			if strings.EqualFold(header, "X-Forwarded-For") {
				parts := strings.Split(v, ",")
				v = strings.TrimSpace(parts[len(parts)-1])
			}
			if a, err := netip.ParseAddr(v); err == nil {
				return a.Unmap(), true
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return a.Unmap(), true
}

// ======================== RATE LIMIT THEO IP (token bucket) ========================

type bucket struct {
	tokens float64
	last   time.Time
}

type ipLimiter struct {
	mu    sync.Mutex
	m     map[netip.Addr]*bucket
	rate  float64 // token / giây
	burst float64
	calls int
}

func newIPLimiter(rpm, burst int) *ipLimiter {
	if rpm <= 0 {
		return nil
	}
	return &ipLimiter{m: make(map[netip.Addr]*bucket), rate: float64(rpm) / 60.0, burst: float64(burst)}
}

// allow trả về (được phép, thời gian chờ gợi ý nếu bị chặn).
func (l *ipLimiter) allow(ip netip.Addr) (bool, time.Duration) {
	if l == nil {
		return true, 0
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	l.calls++
	if l.calls%1000 == 0 {
		for k, b := range l.m {
			if now.Sub(b.last) > 10*time.Minute {
				delete(l.m, k)
			}
		}
	}
	b, ok := l.m[ip]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.m[ip] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false, time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
	}
	b.tokens--
	return true, 0
}

// ======================== MIDDLEWARE ========================

type guard struct {
	allow   *ipAllowlist
	limiter *ipLimiter
	header  string
}

func writeJSONError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Gộp cả 2 định dạng: OpenAI ({"error":{...}}) và Anthropic ({"type":"error","error":{...}}).
	fmt.Fprintf(w, `{"type":"error","error":{"message":%q,"type":%q}}`, msg, typ)
}

// wrap kiểm tra IP trước, rate limit sau (chỉ khi limited=true, tức endpoint tốn quota).
func (g *guard) wrap(next http.HandlerFunc, limited bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip, ok := clientIP(r, g.header)
		if !ok || !g.allow.allows(ip) {
			log.Printf("[Auth] DENY ip=%v path=%s remote=%s xff=%q real-ip=%q", ip, r.URL.Path, r.RemoteAddr,
				r.Header.Get("X-Forwarded-For"), r.Header.Get("X-Real-IP"))
			writeJSONError(w, http.StatusForbidden, "forbidden", "IP not allowed")
			return
		}
		if limited && r.Method == http.MethodPost {
			if okRate, wait := g.limiter.allow(ip); !okRate {
				secs := int(wait.Seconds()) + 1
				w.Header().Set("Retry-After", fmt.Sprint(secs))
				writeJSONError(w, http.StatusTooManyRequests, "rate_limit_error", "Per-IP rate limit exceeded")
				return
			}
		}
		next(w, r)
	}
}
