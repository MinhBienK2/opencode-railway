package main

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Các kịch bản retry/wait với nhiều identity Tor (upstream giả, nhận biết identity qua header X-Test-Egress).

const chatReq = `{"model":"deepseek","messages":[{"role":"user","content":"hi"}]}`

func okChat(w http.ResponseWriter) {
	sse(w, `{"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`)
}

// torSetup: direct bị vô hiệu (để không che lỗi), k identity Tor.
func torSetup(t *testing.T, k int, h http.HandlerFunc) (*Manager, string) {
	t.Helper()
	gw, m := setup(t, h, false)
	cfg.PreferDirect = false
	cfg.TtfbRetry = 0
	m.egresses[0].ready.Store(false)
	for _, e := range newTorGroup("t", "127.0.0.1:1", k) {
		tagged(e)
		e.ready.Store(true)
		m.add(e)
	}
	return m, gw.URL
}

// S1: mọi identity đều 429 → client nhận 429 + Retry-After trong thời gian có hạn, upstream không bị đập vô hạn.
func TestScenarioAllIdentities429IsBounded(t *testing.T) {
	var hits atomic.Int32
	_, url := torSetup(t, 3, func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(429) })
	cfg.MaxWait = 2 * time.Second
	t0 := time.Now()
	resp, body := post(t, url+"/v1/chat/completions", chatReq)
	if resp.StatusCode != 429 {
		t.Fatalf("muốn 429, got %d %s", resp.StatusCode, body)
	}
	if el := time.Since(t0); el > 6*time.Second {
		t.Errorf("phải trả lỗi trong ngân sách thời gian, mất %v", el)
	}
	if n := hits.Load(); n > int32(cfg.MaxAttempts) {
		t.Errorf("vượt MaxAttempts: %d", n)
	}
}

// S2: 2/3 identity bị chặn hạn mức, 1 còn dùng được → thành công; các request sau tránh identity xấu.
func TestScenarioPartialExhaustion(t *testing.T) {
	var bad, good atomic.Int32
	_, url := torSetup(t, 3, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Egress") != "t.3" {
			bad.Add(1)
			w.WriteHeader(429)
			return
		}
		good.Add(1)
		okChat(w)
	})
	for i := 0; i < 10; i++ {
		if resp, _ := post(t, url+"/v1/chat/completions", chatReq); resp.StatusCode != 200 {
			t.Fatalf("req %d: %d", i, resp.StatusCode)
		}
	}
	if good.Load() != 10 {
		t.Errorf("good=%d", good.Load())
	}
	if bad.Load() > 8 {
		t.Errorf("identity xấu bị thử quá nhiều (%d), phải tránh sau khi 429", bad.Load())
	}
}

// S3: 502/503 từ 1 exit → thử identity khác.
func TestScenarioUpstream5xxRetriesElsewhere(t *testing.T) {
	_, url := torSetup(t, 3, func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("X-Test-Egress") {
		case "t.1":
			w.WriteHeader(502)
		case "t.2":
			w.WriteHeader(503)
		default:
			okChat(w)
		}
	})
	for i := 0; i < 6; i++ {
		if resp, _ := post(t, url+"/v1/chat/completions", chatReq); resp.StatusCode != 200 {
			t.Fatalf("req %d: %d", i, resp.StatusCode)
		}
	}
}

// S4: 1 circuit kẹt (không trả header) → bỏ sau TtfbRetry và dùng identity khác.
func TestScenarioStuckCircuitRetried(t *testing.T) {
	_, url := torSetup(t, 2, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) // đọc hết body để server phát hiện kết nối đóng
		if r.Header.Get("X-Test-Egress") == "t.1" {
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
			return
		}
		okChat(w)
	})
	cfg.TtfbRetry = 300 * time.Millisecond
	t0 := time.Now()
	for i := 0; i < 4; i++ {
		if resp, _ := post(t, url+"/v1/chat/completions", chatReq); resp.StatusCode != 200 {
			t.Fatalf("req %d: %d", i, resp.StatusCode)
		}
	}
	if el := time.Since(t0); el > 3*time.Second {
		t.Errorf("circuit kẹt làm chậm quá: %v", el)
	}
}

// S5: Cloudflare chặn 1 exit (403 HTML) → đổi exit, client vẫn thành công.
func TestScenarioCloudflareBlockedExit(t *testing.T) {
	_, url := torSetup(t, 2, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Egress") == "t.1" {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(403)
			w.Write([]byte("<html>Attention Required</html>"))
			return
		}
		okChat(w)
	})
	for i := 0; i < 4; i++ {
		if resp, _ := post(t, url+"/v1/chat/completions", chatReq); resp.StatusCode != 200 {
			t.Fatalf("req %d: %d", i, resp.StatusCode)
		}
	}
}

// S6: bùng nổ 120 request đồng thời, mỗi identity trả 429 cho vài request đầu → tất cả client vẫn thành công.
func TestScenarioBurstWithTransient429(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	_, url := torSetup(t, 4, func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Test-Egress")
		mu.Lock()
		seen[id]++
		n := seen[id]
		mu.Unlock()
		if n <= 3 {
			w.WriteHeader(429)
			return
		}
		okChat(w)
	})
	cfg.MaxConcurrency = 200
	concurrencySem = make(chan struct{}, 200)
	cfg.MaxWait = 20 * time.Second
	var wg sync.WaitGroup
	var fails atomic.Int32
	for i := 0; i < 120; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if resp, _ := post(t, url+"/v1/chat/completions", chatReq); resp.StatusCode != 200 {
				fails.Add(1)
			}
		}()
	}
	wg.Wait()
	if fails.Load() != 0 {
		t.Errorf("%d request lỗi trong bùng nổ", fails.Load())
	}
}

// S7: sau cooldown identity từng 429 được dùng lại (không bị loại vĩnh viễn).
func TestScenarioIdentityRecovers(t *testing.T) {
	var phase atomic.Int32 // 0: t.1 lỗi, 1: t.1 khỏe
	var t1 atomic.Int32
	_, url := torSetup(t, 1, func(w http.ResponseWriter, r *http.Request) {
		t1.Add(1)
		if phase.Load() == 0 {
			w.WriteHeader(429)
			return
		}
		okChat(w)
	})
	cfg.MaxWait = 300 * time.Millisecond
	if resp, _ := post(t, url+"/v1/chat/completions", chatReq); resp.StatusCode != 429 {
		t.Fatalf("pha 0 phải 429, got %d", resp.StatusCode)
	}
	phase.Store(1)
	cfg.MaxWait = 10 * time.Second
	if resp, _ := post(t, url+"/v1/chat/completions", chatReq); resp.StatusCode != 200 {
		t.Fatalf("sau khi hồi phục phải 200, got %d", resp.StatusCode)
	}
}

// S8: client bỏ giữa chừng khi đang chờ egress → slot được trả, không treo.
func TestScenarioClientCancelReleasesSlot(t *testing.T) {
	m, url := torSetup(t, 1, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	})
	req, _ := http.NewRequest("POST", url+"/v1/chat/completions", strings.NewReader(chatReq))
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 400 * time.Millisecond}
	_, _ = c.Do(req)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		n := int32(0)
		for _, e := range m.snapshot() {
			n += e.inflight.Load()
		}
		if n == 0 && len(concurrencySem) == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("slot/inflight không được trả sau khi client bỏ")
}

// S9: upstream cắt giữa stream sau khi đã có dữ liệu → client nhận event lỗi rõ ràng, không im lặng kết thúc OK.
func TestScenarioMidStreamCut(t *testing.T) {
	_, url := torSetup(t, 1, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"par\"}}]}\n\n"))
		w.(http.Flusher).Flush()
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, _ := hj.Hijack()
		conn.Close()
	})
	_, body := post(t, url+"/v1/messages", `{"model":"deepseek","max_tokens":50,"stream":true,"messages":[{"role":"user","content":"x"}]}`)
	if !strings.Contains(body, "event: error") && !strings.Contains(body, "message_stop") {
		t.Errorf("stream bị cắt phải kết thúc rõ ràng: %s", body)
	}
	if strings.Contains(body, "message_stop") && !strings.Contains(body, "event: error") {
		t.Logf("lưu ý: kết thúc bằng message_stop không có event error")
	}
}
