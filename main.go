package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// ======================== CẤU HÌNH & HỒ MÔ HÌNH ========================

const (
	defaultPort = "8080"
	baseZenURL  = "https://opencode.ai/zen/v1"
	baseInfURL  = "https://opencode.ai/zen/v1"
)

var freeModelPool = []string{
	"deepseek-v4.1-flash-free",
	"nemotron-3.5-lightning-free",
	"big-pickle",
	"mimo-v2.6-flash-free",
	"beam-free",
	"minimax-m3.1-flash-free",
	"longcat-2.5-preview-free",
	"glm-5.3-flash-free",
}

var (
	cfg            Config
	mgr            *Manager
	grd            *guard
	poolIndex      uint32
	concurrencySem chan struct{}
	inflightTotal  atomic.Int32

	// có thể ghi đè trong test
	infURL  = baseInfURL
	zenURL  = baseZenURL
	started = time.Now()
)

func isAutoModel(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "auto", "auto-free", "free", "pool", "free-pool", "fast-free":
		return true
	}
	return false
}

func resolveModelName(name string) (resolved string, isPool bool) {
	clean := strings.ToLower(strings.TrimSpace(name))
	if isAutoModel(clean) {
		return freeModelPool[int(atomic.AddUint32(&poolIndex, 1))%len(freeModelPool)], true
	}
	switch clean {
	case "big-pickle":
		return "big-pickle", false
	case "nemotron", "nemotron-3.5-lightning":
		return "nemotron-3.5-lightning-free", false
	case "cat-jam":
		return "cat-jam-free", false
	case "space-bunny":
		return "space-bunny-free", false
	case "minimax", "minimax-m3.1", "minimax-m3.1-flash":
		return "minimax-m3.1-flash-free", false
	case "mimo", "mimo-v2.6-flash":
		return "mimo-v2.6-flash-free", false
	case "deepseek", "deepseek-v4.1-flash", "deepseek-v4.1", "deepseek-flash", "deepseek-chat", "deepseek-reasoner":
		return "deepseek-v4.1-flash-free", false
	case "jev", "jev-1.13":
		return "jev-1.13-free", false
	case "step-5", "step-5-preview":
		return "step-5-preview-free", false
	case "longcat", "longcat-2.5", "longcat-2.5-preview":
		return "longcat-2.5-preview-free", false
	}
	if !strings.HasSuffix(clean, "-free") {
		for _, p := range freeModelPool {
			if strings.TrimSuffix(p, "-free") == clean {
				return p, false
			}
		}
	}
	// Hỗ trợ alias linh hoạt: claude-*, gpt-*, o1*, o3*, sonnet*, opus*, haiku* -> trỏ về defaultModel (deepseek)
	if strings.HasPrefix(clean, "claude") || strings.HasPrefix(clean, "gpt-") ||
		strings.HasPrefix(clean, "o1") || strings.HasPrefix(clean, "o3") ||
		strings.HasPrefix(clean, "sonnet") || strings.HasPrefix(clean, "opus") || strings.HasPrefix(clean, "haiku") {
		def := cfg.DefaultModel
		if def == "" {
			def = "deepseek"
		}
		if def == "deepseek" || def == "deepseek-v4.1-flash" {
			return "deepseek-v4.1-flash-free", false
		}
		return def, false
	}
	return clean, false
}

// applyFingerprint ép stream=true (upstream chỉ nhận stream) và bổ sung bộ tool bắt buộc của opencode
// nếu client chưa khai báo. inject=false thì giữ nguyên tools của client.
func applyFingerprint(body map[string]any, model string, inject bool, stubDesc string) {
	body["stream"] = true
	if !inject {
		if strings.HasPrefix(model, "cat-jam") || strings.HasPrefix(model, "beam") || strings.HasPrefix(model, "space-bunny") || strings.HasPrefix(model, "glm") {
			delete(body, "cache_control")
		}
		return
	}

	d := func(name string) string {
		if stubDesc != "" {
			return stubDesc
		}
		return name
	}
	mandatory := []map[string]any{
		{"type": "function", "function": map[string]any{"name": "bash", "description": d("bash"), "parameters": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}, "required": []string{"command"}}}},
		{"type": "function", "function": map[string]any{"name": "glob", "description": d("glob"), "parameters": map[string]any{"type": "object", "properties": map[string]any{"pattern": map[string]any{"type": "string"}}, "required": []string{"pattern"}}}},
		{"type": "function", "function": map[string]any{"name": "grep", "description": d("grep"), "parameters": map[string]any{"type": "object", "properties": map[string]any{"pattern": map[string]any{"type": "string"}}, "required": []string{"pattern"}}}},
		{"type": "function", "function": map[string]any{"name": "read", "description": d("read"), "parameters": map[string]any{"type": "object", "properties": map[string]any{"filePath": map[string]any{"type": "string"}}, "required": []string{"filePath"}}}},
	}

	raw, has := body["tools"]
	if !has || raw == nil {
		body["tools"] = mandatory
	} else {
		var combined []any
		names := make(map[string]bool)
		if s, ok := raw.([]any); ok {
			for _, t := range s {
				if tm, ok := t.(map[string]any); ok {
					if fn, ok := tm["function"].(map[string]any); ok {
						if n, ok := fn["name"].(string); ok {
							names[n] = true
						}
					}
					combined = append(combined, t)
				}
			}
		}
		for _, mt := range mandatory {
			if n := mt["function"].(map[string]any)["name"].(string); !names[n] {
				combined = append(combined, mt)
			}
		}
		body["tools"] = combined
	}
	if strings.HasPrefix(model, "cat-jam") || strings.HasPrefix(model, "beam") || strings.HasPrefix(model, "space-bunny") || strings.HasPrefix(model, "glm") {
		delete(body, "cache_control")
	}
}

func setCORS(w http.ResponseWriter) {
	if cfg.CORSOrigin == "" {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", cfg.CORSOrigin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, X-Session-Id, x-api-key, anthropic-version, anthropic-beta")
}

// ======================== SSE PARSE / AGGREGATE ========================

type oaToolCall struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type oaChunk struct {
	ID      string `json:"id"`
	Choices []struct {
		Delta struct {
			Content          string       `json:"content"`
			ReasoningContent string       `json:"reasoning_content"`
			ToolCalls        []oaToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens        int `json:"prompt_tokens"`
		CompletionTokens    int `json:"completion_tokens"`
		PromptTokensDetails struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

// readSSE gọi fn cho từng payload "data:" (bỏ [DONE]); không giới hạn độ dài dòng.
func readSSE(r io.Reader, fn func(payload string) error) error {
	br := bufio.NewReaderSize(r, 32*1024)
	for {
		line, err := br.ReadString('\n')
		text := strings.TrimSpace(line)
		if strings.HasPrefix(text, "data:") {
			payload := strings.TrimSpace(strings.TrimPrefix(text, "data:"))
			if payload == "[DONE]" {
				return nil
			}
			if payload != "" {
				if ferr := fn(payload); ferr != nil {
					return ferr
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

type aggTool struct {
	id, name string
	args     strings.Builder
}

type aggregate struct {
	id           string
	content      strings.Builder
	reasoning    strings.Builder
	tools        map[int]*aggTool
	finishReason string
	promptTok    int
	complTok     int
	cachedTok    int
	chunks       int
}

func aggregateSSE(r io.Reader) (*aggregate, error) {
	agg := &aggregate{tools: map[int]*aggTool{}}
	err := readSSE(r, func(payload string) error {
		var ch oaChunk
		if json.Unmarshal([]byte(payload), &ch) != nil {
			return nil
		}
		agg.chunks++
		if ch.ID != "" {
			agg.id = ch.ID
		}
		if ch.Usage != nil {
			agg.promptTok, agg.complTok = ch.Usage.PromptTokens, ch.Usage.CompletionTokens
			agg.cachedTok = ch.Usage.PromptTokensDetails.CachedTokens
		}
		if len(ch.Choices) == 0 {
			return nil
		}
		c := ch.Choices[0]
		agg.content.WriteString(c.Delta.Content)
		agg.reasoning.WriteString(c.Delta.ReasoningContent)
		if c.FinishReason != nil && *c.FinishReason != "" {
			agg.finishReason = *c.FinishReason
		}
		for _, tc := range c.Delta.ToolCalls {
			t := agg.tools[tc.Index]
			if t == nil {
				t = &aggTool{}
				agg.tools[tc.Index] = t
			}
			if tc.ID != "" {
				t.id = tc.ID
			}
			if tc.Function.Name != "" {
				t.name = tc.Function.Name
			}
			t.args.WriteString(tc.Function.Arguments)
		}
		return nil
	})
	return agg, err
}

func (a *aggregate) orderedTools() []*aggTool {
	var out []*aggTool
	for i := 0; i < len(a.tools); i++ {
		if t, ok := a.tools[i]; ok {
			out = append(out, t)
		}
	}
	return out
}

// ======================== HTTP HELPERS ========================

func writeGWError(w http.ResponseWriter, e *gwError) {
	if e.retryAfter > 0 {
		w.Header().Set("Retry-After", fmt.Sprint(int(e.retryAfter.Seconds())+1))
	}
	writeJSONError(w, e.status, e.typ, e.msg)
}

func setGWHeaders(w http.ResponseWriter, up *upstream) {
	w.Header().Set("X-Gateway-Egress", up.egress.Name)
	w.Header().Set("X-Gateway-Model", up.model)
}

func readBody(w http.ResponseWriter, r *http.Request) (map[string]any, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes))
	if err != nil {
		writeJSONError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "Request body too large or unreadable")
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", "Invalid JSON")
		return nil, false
	}
	return m, true
}

// sessionKey nhận diện hội thoại của client để giữ session ID/egress ổn định (cache prefix):
// Claude Code gửi X-Claude-Code-Session-Id và metadata.user_id.session_id; client OpenAI có thể gửi X-Session-Id hoặc user.
func sessionKey(r *http.Request, body map[string]any) string {
	for _, h := range []string{"X-Claude-Code-Session-Id", "X-Session-Id", "X-Opencode-Session"} {
		if v := strings.TrimSpace(r.Header.Get(h)); v != "" {
			return v
		}
	}
	if md, ok := body["metadata"].(map[string]any); ok {
		if uid, ok := md["user_id"].(string); ok && uid != "" {
			var inner struct {
				SessionID string `json:"session_id"`
			}
			if json.Unmarshal([]byte(uid), &inner) == nil && inner.SessionID != "" {
				return inner.SessionID
			}
		}
	}
	if u, ok := body["user"].(string); ok {
		return strings.TrimSpace(u)
	}
	return ""
}

func acquireSlot(w http.ResponseWriter, r *http.Request) (release func(), ok bool) {
	t := time.NewTimer(cfg.QueueWait)
	defer t.Stop()
	select {
	case concurrencySem <- struct{}{}:
		inflightTotal.Add(1)
		return func() { <-concurrencySem; inflightTotal.Add(-1) }, true
	case <-t.C:
		w.Header().Set("Retry-After", "5")
		writeJSONError(w, http.StatusServiceUnavailable, "overloaded_error", "Gateway is at capacity, retry shortly")
		return nil, false
	case <-r.Context().Done():
		return nil, false
	}
}

func passthroughError(w http.ResponseWriter, up *upstream) {
	b, _ := io.ReadAll(io.LimitReader(up.resp.Body, 1<<20))
	setGWHeaders(w, up)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(up.resp.StatusCode)
	_, _ = w.Write(b)
}

func preflight(w http.ResponseWriter, r *http.Request) bool {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "invalid_request_error", "Method not allowed")
		return true
	}
	return false
}

// ======================== HANDLERS ========================

func chatHandler(w http.ResponseWriter, r *http.Request) {
	if preflight(w, r) {
		return
	}
	reqData, ok := readBody(w, r)
	if !ok {
		return
	}
	if ms, _ := reqData["messages"].([]any); len(ms) == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", "messages: at least one message is required")
		return
	}
	release, ok := acquireSlot(w, r)
	if !ok {
		return
	}
	defer release()

	modelIn, _ := reqData["model"].(string)
	clientStream, _ := reqData["stream"].(bool)
	target, isPool := resolveModelName(modelIn)
	t0 := time.Now()

	up, gerr := forward(r.Context(), target, isPool, infURL+"/chat/completions", "*/*", sessionKey(r, reqData), func(m string) []byte {
		reqData["model"] = m
		applyFingerprint(reqData, m, true, stubDescription)
		b, _ := json.Marshal(reqData)
		return b
	})
	if gerr != nil {
		writeGWError(w, gerr)
		return
	}
	defer up.close()
	if up.resp.StatusCode != http.StatusOK {
		passthroughError(w, up)
		return
	}
	setGWHeaders(w, up)

	if clientStream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher, _ := w.(http.Flusher)
		br := bufio.NewReaderSize(up.resp.Body, 32*1024)
		var firstByte time.Duration
		for {
			line, err := br.ReadBytes('\n')
			if len(line) > 0 {
				if firstByte == 0 {
					firstByte = time.Since(t0)
				}
				if _, werr := w.Write(line); werr != nil {
					break
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
			if err != nil {
				break
			}
		}
		logReq("chat", "stream", up, t0, firstByte)
		return
	}

	agg, err := aggregateSSE(up.resp.Body)
	if err != nil && agg.chunks == 0 {
		writeJSONError(w, http.StatusBadGateway, "upstream_error", "Upstream stream interrupted")
		return
	}
	msg := map[string]any{
		"role":              "assistant",
		"content":           agg.content.String(),
		"reasoning_content": agg.reasoning.String(),
	}
	finish := "stop"
	if tools := agg.orderedTools(); len(tools) > 0 {
		var list []any
		for i, t := range tools {
			list = append(list, map[string]any{
				"index": i, "id": t.id, "type": "function",
				"function": map[string]any{"name": t.name, "arguments": t.args.String()},
			})
		}
		msg["tool_calls"] = list
		finish = "tool_calls"
	}
	out, _ := json.Marshal(map[string]any{
		"id": agg.id, "object": "chat.completion", "created": time.Now().Unix(), "model": up.model,
		"choices": []map[string]any{{"index": 0, "message": msg, "finish_reason": finish}},
		"usage": map[string]any{"prompt_tokens": agg.promptTok, "completion_tokens": agg.complTok, "total_tokens": agg.promptTok + agg.complTok,
			"prompt_tokens_details": map[string]any{"cached_tokens": agg.cachedTok}},
	})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
	logReqTok("chat", "json", up, t0, up.ttfb, agg.promptTok, agg.cachedTok)
}

func logReq(ep, mode string, up *upstream, t0 time.Time, first time.Duration) {
	logReqTok(ep, mode, up, t0, first, 0, 0)
}

// logReqTok ghi thêm số token prompt và token trúng cache (nếu upstream báo) để đo hiệu quả prompt cache.
func logReqTok(ep, mode string, up *upstream, t0 time.Time, first time.Duration, prompt, cached int) {
	log.Printf("[req] %s %s model=%s egress=%s ttfb=%v first=%v total=%v prompt=%d cached=%d",
		ep, mode, up.model, up.egress.Name, up.ttfb.Round(time.Millisecond), first.Round(time.Millisecond), time.Since(t0).Round(time.Millisecond), prompt, cached)
}

func systemoneHandler(w http.ResponseWriter, r *http.Request) {
	if preflight(w, r) {
		return
	}
	reqData, ok := readBody(w, r)
	if !ok {
		return
	}
	release, ok := acquireSlot(w, r)
	if !ok {
		return
	}
	defer release()

	model, _ := reqData["model"].(string)
	resolved, _ := resolveModelName(model)
	if resolved == "" || resolved == "jev" || resolved == "jev-1.13" {
		resolved = "jev-1.13-free"
	}
	reqData["model"] = resolved
	body, _ := json.Marshal(reqData)
	t0 := time.Now()

	up, gerr := forward(r.Context(), resolved, false, zenURL+"/systemone", "application/json", sessionKey(r, reqData), func(string) []byte { return body })
	if gerr != nil {
		writeGWError(w, gerr)
		return
	}
	defer up.close()
	setGWHeaders(w, up)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(up.resp.StatusCode)
	_, _ = io.Copy(w, up.resp.Body)
	logReq("systemone", "json", up, t0, up.ttfb)
}

var modelList = []struct{ id, owner string }{
	{"auto-free", "auto-dispatcher"}, {"auto", "auto-dispatcher"},
	{"nemotron-3.5-lightning", "opencode"}, {"big-pickle", "opencode"}, {"cat-jam", "opencode"},
	{"space-bunny", "opencode"}, {"mimo-v2.6-flash", "opencode"}, {"deepseek", "opencode"},
	{"deepseek-v4.1-flash", "opencode"}, {"jev-1.13", "typesafe"}, {"step-5-preview", "opencode"},
	{"longcat-2.5-preview", "opencode"},
}

// modelObj đồng thời hợp lệ với định dạng OpenAI (object/owned_by) và Anthropic (type/display_name/created_at).
func modelObj(id, owner string) map[string]any {
	return map[string]any{"id": id, "object": "model", "owned_by": owner, "created": 1735689600,
		"type": "model", "display_name": id, "created_at": "2025-01-01T00:00:00Z"}
}

func modelsHandler(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if id := strings.TrimPrefix(r.URL.Path, "/v1/models/"); id != "" && id != r.URL.Path {
		want := anthropicModel(id)
		for _, m := range modelList {
			if m.id == want || m.id == strings.ToLower(id) {
				out, _ := json.Marshal(modelObj(m.id, m.owner))
				_, _ = w.Write(out)
				return
			}
		}
		writeAnthropicError(w, &gwError{status: http.StatusNotFound, typ: "not_found_error", msg: "model not found: " + id})
		return
	}
	var models []map[string]any
	for _, m := range modelList {
		models = append(models, modelObj(m.id, m.owner))
	}
	out, _ := json.Marshal(map[string]any{"object": "list", "data": models, "has_more": false,
		"first_id": modelList[0].id, "last_id": modelList[len(modelList)-1].id})
	_, _ = w.Write(out)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/health" {
		writeJSONError(w, http.StatusNotFound, "not_found_error", "Unknown endpoint: "+r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok","gateway":"opencode-railway"}`))
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	type egStat struct {
		Name      string             `json:"name"`
		Kind      string             `json:"kind"`
		Ready     bool               `json:"ready"`
		Inflight  int32              `json:"inflight"`
		Requests  int64              `json:"requests"`
		OK        int64              `json:"ok"`
		RateLimit int64              `json:"rate_limited"`
		Errors    int64              `json:"errors"`
		Cooldown  map[string]float64 `json:"cooldown_seconds,omitempty"`
	}
	var list []egStat
	for _, e := range mgr.snapshot() {
		s := egStat{Name: e.Name, Kind: e.Kind, Ready: e.ready.Load(), Inflight: e.inflight.Load(),
			Requests: e.reqs.Load(), OK: e.oks.Load(), RateLimit: e.r429.Load(), Errors: e.errs.Load()}
		e.mu.Lock()
		for model, c := range e.cool {
			if d := time.Until(c.until); d > 0 {
				if s.Cooldown == nil {
					s.Cooldown = map[string]float64{}
				}
				s.Cooldown[model] = d.Round(time.Second).Seconds()
			}
		}
		e.mu.Unlock()
		list = append(list, s)
	}
	out, _ := json.Marshal(map[string]any{
		"uptime_seconds": int(time.Since(started).Seconds()),
		"inflight":       inflightTotal.Load(),
		"max_inflight":   cfg.MaxConcurrency,
		"egress":         list,
	})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

// ======================== MAIN ========================

func buildMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", grd.wrap(chatHandler, true))
	mux.HandleFunc("/v1/messages", grd.wrap(messagesHandler, true))
	mux.HandleFunc("/v1/responses", grd.wrap(responsesHandler, true))
	mux.HandleFunc("/v1/completions", grd.wrap(completionsHandler, true))
	mux.HandleFunc("/v1/systemone", grd.wrap(systemoneHandler, true))
	mux.HandleFunc("/v1/messages/count_tokens", grd.wrap(countTokensHandler, false))
	mux.HandleFunc("/v1/models", grd.wrap(modelsHandler, false))
	mux.HandleFunc("/v1/models/", grd.wrap(modelsHandler, false))
	mux.HandleFunc("/status", grd.wrap(statusHandler, false))
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/", healthHandler)
	return mux
}

func main() {
	cfg = loadConfig()
	// Dọn dẹp các thư mục tạm tor cũ từ phiên trước nếu có
	if matches, err := filepath.Glob(filepath.Join(os.TempDir(), "tor-gw-*")); err == nil {
		for _, d := range matches {
			_ = os.RemoveAll(d)
		}
	}
	concurrencySem = make(chan struct{}, cfg.MaxConcurrency)

	allow, err := parseAllowlist(cfg.AllowedIPs)
	if err != nil {
		log.Fatalf("Cấu hình sai: %v", err)
	}
	grd = &guard{allow: allow, limiter: newIPLimiter(cfg.RateRPM, cfg.RateBurst), header: cfg.ClientIPHeader}
	switch {
	case allow.all:
		log.Printf("[Auth] ALLOWED_IPS=* => KHÔNG giới hạn IP (chỉ nên dùng khi đã có lớp bảo vệ khác)")
	case allow.empty():
		log.Printf("[Auth] ALLOWED_IPS trống => chỉ cho loopback và mạng nội bộ. Đặt ALLOWED_IPS=ip1,ip2,cidr để mở cho client khác")
	default:
		log.Printf("[Auth] ALLOWED_IPS: %d mục", len(allow.prefixes))
	}
	if cfg.ClientIPHeader != "" {
		log.Printf("[Auth] Lấy IP client từ header %s", cfg.ClientIPHeader)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mgr = newManager(ctx)
	mgr.addStatic(os.Getenv("TOR_INSTANCES"), os.Getenv("PROXY_POOL"))
	torAvailable = checkTorBinary()
	mode := "Tor trước (nhiều identity/daemon), direct dự phòng"
	if cfg.PreferDirect {
		mode = "direct trước, Tor khi bị 429"
	}
	switch {
	case cfg.ForceTor:
		mode = "chỉ Tor (FORCE_TOR)"
	case !torAvailable && len(mgr.snapshot()) == 1:
		mode = "chỉ direct"
	}
	log.Printf("Egress: %s | TOR_WARM=%d TOR_MAX=%d (tự thêm daemon khi tải > %d/daemon, tắt sau %v nhàn rỗi)", mode, cfg.TorWarm, cfg.TorMax, cfg.TorLoad, cfg.TorIdleStop)
	go mgr.warmDirect()
	if torAvailable {
		go mgr.warmTor()
		go mgr.reapIdleTor()
		go mgr.autoscaleTor()
	}

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           buildMux(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("Opencode Railway Gateway listening on :%s (in-flight tối đa %d)", cfg.Port, cfg.MaxConcurrency)

	select {
	case err := <-errCh:
		log.Fatalf("Server error: %v", err)
	case <-ctx.Done():
		log.Printf("Nhận tín hiệu dừng, đang đóng...")
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	mgr.wg.Wait()
}
