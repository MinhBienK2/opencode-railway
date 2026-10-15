package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ======================== CẤU HÌNH & HỒ MÔ HÌNH (MODEL POOL) ========================

const (
	defaultPort = "8080"
	baseZenURL  = "https://opencode.ai/zen/v1"
	baseInfURL  = "https://opencode.ai/inference/v1"
)

var freeModelPool = []string{
	"nemotron-3.5-lightning-free",
	"big-pickle",
	"cat-jam-free",
	"space-bunny-free",
	"mimo-v2.6-flash-free",
	"step-5-preview-free",
	"longcat-2.5-preview-free",
}

var (
	poolIndex    uint32
	laneCooldown sync.Map // modelName -> time.Time
	idLock       sync.Mutex
	lastMs       int64
	idCounter    int64
)
type TorInstance struct {
	SocksAddr   string
	ControlAddr string
	Client      *http.Client
}

var (
	maxConcurrency = getEnvInt("MAX_CONCURRENCY", 30)
	concurrencySem = make(chan struct{}, maxConcurrency)
	torInstances   []*TorInstance
	torIndex       uint32
)

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return defaultVal
}

func signalNewnym(controlAddr string) {
	if controlAddr == "" {
		return
	}
	conn, err := net.DialTimeout("tcp", controlAddr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	fmt.Fprintf(conn, "AUTHENTICATE \"\"\r\n")
	buf := make([]byte, 512)
	n, _ := conn.Read(buf)
	if !strings.HasPrefix(string(buf[:n]), "250") {
		return
	}

	fmt.Fprintf(conn, "SIGNAL NEWNYM\r\n")
	n, _ = conn.Read(buf)
	if strings.HasPrefix(string(buf[:n]), "250") {
		log.Printf("[Tor Control] Switched circuit on %s (NEWNYM OK)", controlAddr)
	}
}

func initProxyAndTorPool() {
	torEnv := os.Getenv("TOR_INSTANCES")
	if torEnv != "" {
		for _, item := range strings.Split(torEnv, ",") {
			parts := strings.Split(strings.TrimSpace(item), ":")
			if len(parts) >= 2 {
				host := parts[0]
				socksPort := parts[1]
				controlPort := ""
				if len(parts) >= 3 {
					controlPort = parts[2]
				}
				socksURL, err := url.Parse(fmt.Sprintf("socks5://%s:%s", host, socksPort))
				if err == nil {
					transport := &http.Transport{
						Proxy: http.ProxyURL(socksURL),
						DialContext: (&net.Dialer{
							Timeout:   15 * time.Second,
							KeepAlive: 30 * time.Second,
						}).DialContext,
						ForceAttemptHTTP2:     true,
						MaxIdleConns:          64,
						MaxIdleConnsPerHost:   16,
						IdleConnTimeout:       90 * time.Second,
						TLSHandshakeTimeout:   15 * time.Second,
						ExpectContinueTimeout: 1 * time.Second,
					}
					client := &http.Client{Transport: transport, Timeout: 120 * time.Second}
					ctrlAddr := ""
					if controlPort != "" {
						ctrlAddr = fmt.Sprintf("%s:%s", host, controlPort)
					}
					torInstances = append(torInstances, &TorInstance{
						SocksAddr:   fmt.Sprintf("%s:%s", host, socksPort),
						ControlAddr: ctrlAddr,
						Client:      client,
					})
				}
			}
		}
	}

	raw := os.Getenv("PROXY_POOL")
	if raw != "" {
		for _, p := range strings.Split(raw, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if u, err := url.Parse(p); err == nil {
				transport := &http.Transport{
					Proxy: http.ProxyURL(u),
					DialContext: (&net.Dialer{
						Timeout:   15 * time.Second,
						KeepAlive: 30 * time.Second,
					}).DialContext,
					ForceAttemptHTTP2:     true,
					MaxIdleConns:          64,
					MaxIdleConnsPerHost:   16,
					IdleConnTimeout:       90 * time.Second,
					TLSHandshakeTimeout:   15 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
				}
				client := &http.Client{Transport: transport, Timeout: 120 * time.Second}
				torInstances = append(torInstances, &TorInstance{
					SocksAddr: u.Host,
					Client:    client,
				})
			}
		}
	}

	if len(torInstances) > 0 {
		log.Printf("Proxy/Tor Pool initialized with %d active egress instances", len(torInstances))
	} else {
		log.Printf("No Tor/Proxy configured, running in 100%% Direct IP mode")
	}
}

func getEgressInstance() (*http.Client, string) {
	if len(torInstances) == 0 {
		return sharedClient, ""
	}
	idx := atomic.AddUint32(&torIndex, 1) % uint32(len(torInstances))
	inst := torInstances[idx]
	return inst.Client, inst.ControlAddr
}

var (
	sharedTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	sharedClient = &http.Client{
		Transport: sharedTransport,
		Timeout:   120 * time.Second,
	}
)

func markLaneCooldown(model string, d time.Duration) {
	laneCooldown.Store(model, time.Now().Add(d))
}

func isLaneCoolingDown(model string) bool {
	if val, ok := laneCooldown.Load(model); ok {
		if until, ok := val.(time.Time); ok {
			return time.Now().Before(until)
		}
	}
	return false
}

func selectNextModel(tried map[string]bool) string {
	n := uint32(len(freeModelPool))
	start := atomic.AddUint32(&poolIndex, 1)

	// Ưu tiên 1: Chưa thử trong request này VÀ không đang trong soft cooldown
	for i := range n {
		candidate := freeModelPool[(start+i)%n]
		if (tried == nil || !tried[candidate]) && !isLaneCoolingDown(candidate) {
			return candidate
		}
	}

	// Ưu tiên 2: Bất kỳ model nào chưa thử trong request này
	if tried != nil {
		for i := range n {
			candidate := freeModelPool[(start+i)%n]
			if !tried[candidate] {
				return candidate
			}
		}
	}

	return freeModelPool[start%n]
}

func isAutoModel(name string) bool {
	m := strings.ToLower(strings.TrimSpace(name))
	switch m {
	case "auto", "auto-free", "free", "pool", "free-pool", "fast-free":
		return true
	default:
		return false
	}
}

func resolveModelName(name string) (resolved string, isPool bool) {
	clean := strings.ToLower(strings.TrimSpace(name))
	if isAutoModel(clean) {
		return selectNextModel(nil), true
	}
	// Chuẩn hóa tên model lẻ
	switch clean {
	case "big-pickle":
		return "big-pickle", false
	case "nemotron", "nemotron-3.5-lightning":
		return "nemotron-3.5-lightning-free", false
	case "cat-jam":
		return "cat-jam-free", false
	case "space-bunny":
		return "space-bunny-free", false
	case "mimo", "mimo-v2.6-flash":
		return "mimo-v2.6-flash-free", false
	case "jev", "jev-1.13":
		return "jev-1.13-free", false
	case "step-5", "step-5-preview":
		return "step-5-preview-free", false
	case "longcat", "longcat-2.5", "longcat-2.5-preview":
		return "longcat-2.5-preview-free", false
	default:
		if !strings.HasSuffix(clean, "-free") && clean != "big-pickle" {
			// Thử map qua đuôi -free nếu có
			for _, p := range freeModelPool {
				if strings.TrimSuffix(p, "-free") == clean {
					return p, false
				}
			}
		}
		return clean, false
	}
}

// ======================== TẠO FINGERPRINT OPENCODE 1.19.0 ========================

const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func makeID(descending bool) string {
	now := time.Now().UnixMilli()
	idLock.Lock()
	if now != lastMs {
		lastMs = now
		idCounter = 0
	}
	idCounter++
	c := idCounter & 0xFFF
	idLock.Unlock()

	val := (uint64(now)*0x1000 + uint64(c)) & 0xFFFFFFFFFFFF
	if descending {
		val = (^val) & 0xFFFFFFFFFFFF
	}

	var randBytes [14]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		var seed [8]byte
		binary.BigEndian.PutUint64(seed[:], uint64(time.Now().UnixNano()))
		for i := range randBytes {
			randBytes[i] = seed[i%8] ^ byte(i*37)
		}
	}
	var suffix [14]byte
	for i, b := range randBytes {
		suffix[i] = alphabet[b%byte(len(alphabet))]
	}
	return fmt.Sprintf("%012x%s", val, string(suffix[:]))
}

func newSessionID() string { return "ses_" + makeID(true) }
func newRequestID() string { return "msg_" + makeID(false) }

func newProjectID() string { return randomHex(40) }

func randomHex(n int) string {
	b := make([]byte, n/2)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func applyFingerprint(body map[string]any, model string) {
	body["stream"] = true

	mandatoryTools := []map[string]any{
		{"type": "function", "function": map[string]any{"name": "bash", "description": "bash", "parameters": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}, "required": []string{"command"}}}},
		{"type": "function", "function": map[string]any{"name": "glob", "description": "glob", "parameters": map[string]any{"type": "object", "properties": map[string]any{"pattern": map[string]any{"type": "string"}}, "required": []string{"pattern"}}}},
		{"type": "function", "function": map[string]any{"name": "grep", "description": "grep", "parameters": map[string]any{"type": "object", "properties": map[string]any{"pattern": map[string]any{"type": "string"}}, "required": []string{"pattern"}}}},
		{"type": "function", "function": map[string]any{"name": "read", "description": "read", "parameters": map[string]any{"type": "object", "properties": map[string]any{"filePath": map[string]any{"type": "string"}}, "required": []string{"filePath"}}}},
	}

	existingToolsRaw, hasTools := body["tools"]
	if !hasTools || existingToolsRaw == nil {
		body["tools"] = mandatoryTools
	} else {
		var combined []any
		names := make(map[string]bool)

		if toolsSlice, ok := existingToolsRaw.([]any); ok {
			for _, t := range toolsSlice {
				if tMap, ok := t.(map[string]any); ok {
					if fn, ok := tMap["function"].(map[string]any); ok {
						if name, ok := fn["name"].(string); ok {
							names[name] = true
						}
					}
					combined = append(combined, t)
				}
			}
		}

		for _, mt := range mandatoryTools {
			fn := mt["function"].(map[string]any)
			name := fn["name"].(string)
			if !names[name] {
				combined = append(combined, mt)
			}
		}
		body["tools"] = combined
	}

	if strings.HasPrefix(model, "cat-jam") {
		delete(body, "cache_control")
	}
}

// ======================== HTTP HANDLERS ========================

func chatHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	var reqData map[string]any
	if err := json.Unmarshal(bodyBytes, &reqData); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	select {
	case concurrencySem <- struct{}{}:
		defer func() { <-concurrencySem }()
	case <-r.Context().Done():
		http.Error(w, "Client disconnected", http.StatusRequestTimeout)
		return
	}


	modelIn, _ := reqData["model"].(string)
	clientStream, _ := reqData["stream"].(bool)

	targetModel, isPool := resolveModelName(modelIn)
	maxAttempts := 3
	if isPool {
		maxAttempts = len(freeModelPool)
	}

	triedModels := make(map[string]bool)

	for attempt := range maxAttempts {
		triedModels[targetModel] = true
		reqData["model"] = targetModel
		applyFingerprint(reqData, targetModel)

		upstreamBytes, _ := json.Marshal(reqData)
		upstreamURL := baseInfURL + "/chat/completions"

		upReq, err := http.NewRequestWithContext(r.Context(), "POST", upstreamURL, bytes.NewReader(upstreamBytes))
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		sesID := newSessionID()
		upReq.Header.Set("Authorization", "Bearer public")
		upReq.Header.Set("Content-Type", "application/json")
		upReq.Header.Set("User-Agent", "opencode/1.19.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14")
		upReq.Header.Set("x-opencode-client", "cli")
		upReq.Header.Set("x-opencode-project", newProjectID())
		upReq.Header.Set("x-opencode-request", newRequestID())
		upReq.Header.Set("x-opencode-session", sesID)
		upReq.Header.Set("x-opencode-session-id", sesID)
		upReq.Header.Set("x-session-id", sesID)
		upReq.Header.Set("Accept", "*/*")

		client, ctrlAddr := getEgressInstance()
		resp, err := client.Do(upReq)
		if err != nil {
			log.Printf("[Attempt %d] Transport error on %s: %v", attempt, targetModel, err)
			markLaneCooldown(targetModel, 5*time.Second)
			if ctrlAddr != "" {
				go signalNewnym(ctrlAddr)
			}
			if attempt+1 < maxAttempts {
				if isPool {
					targetModel = selectNextModel(triedModels)
				}
				continue
			}
			http.Error(w, "Upstream transport error", http.StatusBadGateway)
			return
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusBadGateway {
			resp.Body.Close()
			markLaneCooldown(targetModel, 8*time.Second)
			log.Printf("[Attempt %d] Rate limit (HTTP %d) on %s -> Rotating immediately...", attempt, resp.StatusCode, targetModel)
			if ctrlAddr != "" {
				go signalNewnym(ctrlAddr)
			}
			if attempt+1 < maxAttempts {
				if isPool {
					targetModel = selectNextModel(triedModels)
				} else {
					time.Sleep(60 * time.Millisecond)
				}
				continue
			}
			http.Error(w, `{"error":{"message":"All free model lanes temporarily busy. Please retry shortly.","type":"rate_limit_error"}}`, http.StatusTooManyRequests)
			return
		}

		if resp.StatusCode != http.StatusOK {
			errBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.StatusCode)
			w.Write(errBody)
			return
		}

		// Thành công! Đánh dấu lane khỏe mạnh
		laneCooldown.Delete(targetModel)
		defer resp.Body.Close()

		if clientStream {
			// Truyền trực tiếp luồng SSE cho client
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			flusher, ok := w.(http.Flusher)
			reader := bufio.NewReaderSize(resp.Body, 32*1024)
			for {
				line, err := reader.ReadBytes('\n')
				if len(line) > 0 {
					w.Write(line)
					if ok {
						flusher.Flush()
					}
				}
				if err != nil {
					break
				}
			}
			return
		}

		// Tổng hợp SSE thành JSON cho client non-stream (zero-copy buffer)
		var finalContent, reasoningContent strings.Builder
		var finalID string
		var toolCallsMap = make(map[int]map[string]any)

		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			text := strings.TrimSpace(scanner.Text())
			if !strings.HasPrefix(text, "data: ") || text == "data: [DONE]" {
				continue
			}
			var chunk struct {
				ID      string `json:"id"`
				Choices []struct {
					Delta struct {
						Content          string `json:"content"`
						ReasoningContent string `json:"reasoning_content"`
						ToolCalls        []struct {
							Index    int    `json:"index"`
							ID       string `json:"id"`
							Type     string `json:"type"`
							Function struct {
								Name      string `json:"name"`
								Arguments string `json:"arguments"`
							} `json:"function"`
						} `json:"tool_calls"`
					} `json:"delta"`
				} `json:"choices"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(text, "data: ")), &chunk); err == nil {
				if chunk.ID != "" {
					finalID = chunk.ID
				}
				if len(chunk.Choices) > 0 {
					d := chunk.Choices[0].Delta
					finalContent.WriteString(d.Content)
					reasoningContent.WriteString(d.ReasoningContent)
					for _, tc := range d.ToolCalls {
						entry, exists := toolCallsMap[tc.Index]
						if !exists {
							entry = map[string]any{
								"index": tc.Index,
								"id":    tc.ID,
								"type":  tc.Type,
								"function": map[string]any{
									"name":      tc.Function.Name,
									"arguments": tc.Function.Arguments,
								},
							}
							toolCallsMap[tc.Index] = entry
						} else {
							fn := entry["function"].(map[string]any)
							if tc.ID != "" {
								entry["id"] = tc.ID
							}
							if tc.Function.Name != "" {
								fn["name"] = tc.Function.Name
							}
							fn["arguments"] = fn["arguments"].(string) + tc.Function.Arguments
						}
					}
				}
			}
		}

		var toolCallsList []any
		if len(toolCallsMap) > 0 {
			for i := range len(toolCallsMap) {
				if tc, ok := toolCallsMap[i]; ok {
					toolCallsList = append(toolCallsList, tc)
				}
			}
		}

		msg := map[string]any{
			"role":              "assistant",
			"content":           finalContent.String(),
			"reasoning_content": reasoningContent.String(),
		}
		finishReason := "stop"
		if len(toolCallsList) > 0 {
			msg["tool_calls"] = toolCallsList
			finishReason = "tool_calls"
		}

		outJSON, _ := json.Marshal(map[string]any{
			"id":      finalID,
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   targetModel,
			"choices": []map[string]any{{
				"index":         0,
				"message":       msg,
				"finish_reason": finishReason,
			}},
		})

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(outJSON)
		return
	}
}

func systemoneHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	var reqData map[string]any
	if err := json.Unmarshal(bodyBytes, &reqData); err == nil {
		model, _ := reqData["model"].(string)
		resolved, _ := resolveModelName(model)
		if resolved == "" || resolved == "jev" || resolved == "jev-1.13" {
			resolved = "jev-1.13-free"
		}
		reqData["model"] = resolved
		bodyBytes, _ = json.Marshal(reqData)
	}

	upstreamURL := baseZenURL + "/systemone"
	maxAttempts := 3
	for attempt := range maxAttempts {
		client, ctrlAddr := getEgressInstance()
		sesID := newSessionID()
		upReq, err := http.NewRequestWithContext(r.Context(), "POST", upstreamURL, bytes.NewReader(bodyBytes))
		if err != nil {
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}

		upReq.Header.Set("Authorization", "Bearer public")
		upReq.Header.Set("Content-Type", "application/json")
		upReq.Header.Set("User-Agent", "opencode/1.19.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14")
		upReq.Header.Set("x-opencode-client", "cli")
		upReq.Header.Set("x-opencode-project", newProjectID())
		upReq.Header.Set("x-opencode-request", newRequestID())
		upReq.Header.Set("x-opencode-session", sesID)
		upReq.Header.Set("x-opencode-session-id", sesID)
		upReq.Header.Set("x-session-id", sesID)
		upReq.Header.Set("Accept", "application/json")

		resp, err := client.Do(upReq)
		if err != nil {
			if ctrlAddr != "" {
				go signalNewnym(ctrlAddr)
			}
			if attempt+1 < maxAttempts {
				continue
			}
			http.Error(w, "Upstream timeout", http.StatusBadGateway)
			return
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusBadGateway {
			resp.Body.Close()
			if ctrlAddr != "" {
				go signalNewnym(ctrlAddr)
			}
			if attempt+1 < maxAttempts {
				continue
			}
			http.Error(w, `{"error":{"message":"JEV rate limit exceeded","type":"rate_limit_error"}}`, http.StatusTooManyRequests)
			return
		}

		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}
}

func modelsHandler(w http.ResponseWriter, r *http.Request) {
	models := []map[string]any{
		{"id": "auto-free", "object": "model", "owned_by": "auto-dispatcher"},
		{"id": "auto", "object": "model", "owned_by": "auto-dispatcher"},
		{"id": "nemotron-3.5-lightning", "object": "model", "owned_by": "opencode"},
		{"id": "big-pickle", "object": "model", "owned_by": "opencode"},
		{"id": "cat-jam", "object": "model", "owned_by": "opencode"},
		{"id": "space-bunny", "object": "model", "owned_by": "opencode"},
		{"id": "mimo-v2.6-flash", "object": "model", "owned_by": "opencode"},
		{"id": "jev-1.13", "object": "model", "owned_by": "typesafe"},
		{"id": "step-5-preview", "object": "model", "owned_by": "opencode"},
		{"id": "longcat-2.5-preview", "object": "model", "owned_by": "opencode"},
	}
	out, _ := json.Marshal(map[string]any{
		"object": "list",
		"data":   models,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok","gateway":"opencode-railway","concurrency":"unbounded"}`))
}

// ======================== MAIN ========================

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	http.HandleFunc("/v1/chat/completions", chatHandler)
	http.HandleFunc("/v1/systemone", systemoneHandler)
	http.HandleFunc("/v1/models", modelsHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/", healthHandler)

	initProxyAndTorPool()
	log.Printf("In-flight concurrency limit: %d", maxConcurrency)
	log.Printf("Opencode Railway Gateway listening on port :%s", port)
	log.Printf("Available Pool Models: %v", freeModelPool)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
