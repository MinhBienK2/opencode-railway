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
	"net/http"
	"os"
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
}

var (
	poolIndex    uint32
	poolCooldown sync.Map // modelID -> time.Time
	idLock       sync.Mutex
	lastMs       int64
	idCounter    int64
	projectID    = randomHex(40)
)

func markCooldown(model string, d time.Duration) {
	poolCooldown.Store(model, time.Now().Add(d))
}

func isCoolingDown(model string) bool {
	if val, ok := poolCooldown.Load(model); ok {
		if until, ok := val.(time.Time); ok {
			return time.Now().Before(until)
		}
	}
	return false
}

func selectNextModel(exclude string) string {
	n := uint32(len(freeModelPool))
	start := atomic.AddUint32(&poolIndex, 1)

	// Ưu tiên 1: Chọn model khỏe mạnh và không bị trùng
	for i := range n {
		candidate := freeModelPool[(start+i)%n]
		if candidate != exclude && !isCoolingDown(candidate) {
			return candidate
		}
	}
	// Ưu tiên 2: Chọn bất kỳ model nào khác exclude
	if exclude != "" {
		for i := range n {
			candidate := freeModelPool[(start+i)%n]
			if candidate != exclude {
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
		return selectNextModel(""), true
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

func randomHex(n int) string {
	b := make([]byte, n/2)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func applyFingerprint(body map[string]any, model string) {
	body["stream"] = true
	if _, ok := body["tools"]; !ok {
		body["tools"] = []map[string]any{
			{"type": "function", "function": map[string]any{"name": "bash", "description": "bash", "parameters": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}, "required": []string{"command"}}}},
			{"type": "function", "function": map[string]any{"name": "glob", "description": "glob", "parameters": map[string]any{"type": "object", "properties": map[string]any{"pattern": map[string]any{"type": "string"}}, "required": []string{"pattern"}}}},
			{"type": "function", "function": map[string]any{"name": "grep", "description": "grep", "parameters": map[string]any{"type": "object", "properties": map[string]any{"pattern": map[string]any{"type": "string"}}, "required": []string{"pattern"}}}},
			{"type": "function", "function": map[string]any{"name": "read", "description": "read", "parameters": map[string]any{"type": "object", "properties": map[string]any{"filePath": map[string]any{"type": "string"}}, "required": []string{"filePath"}}}},
		}
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

	modelIn, _ := reqData["model"].(string)
	clientStream, _ := reqData["stream"].(bool)

	targetModel, isPool := resolveModelName(modelIn)
	maxAttempts := 3
	if isPool {
		maxAttempts = len(freeModelPool)
	}

	for attempt := range maxAttempts {
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
		upReq.Header.Set("x-opencode-project", projectID)
		upReq.Header.Set("x-opencode-request", newRequestID())
		upReq.Header.Set("x-opencode-session", sesID)
		upReq.Header.Set("x-opencode-session-id", sesID)
		upReq.Header.Set("x-session-id", sesID)
		upReq.Header.Set("Accept", "*/*")

		client := &http.Client{Timeout: 90 * time.Second}
		resp, err := client.Do(upReq)
		if err != nil {
			log.Printf("[Attempt %d] Transport error on %s: %v", attempt, targetModel, err)
			if attempt+1 < maxAttempts {
				targetModel = selectNextModel(targetModel)
				time.Sleep(300 * time.Millisecond)
				continue
			}
			http.Error(w, "Upstream transport error", http.StatusBadGateway)
			return
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusBadGateway {
			resp.Body.Close()
			markCooldown(targetModel, 15*time.Second)
			log.Printf("[Attempt %d] Rate limit (HTTP %d) on %s -> Rotating...", attempt, resp.StatusCode, targetModel)
			if attempt+1 < maxAttempts {
				targetModel = selectNextModel(targetModel)
				time.Sleep(400 * time.Millisecond)
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

		// Thành công! Phản hồi lại cho client
		defer resp.Body.Close()

		if clientStream {
			// Truyền trực tiếp luồng SSE cho client
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			flusher, ok := w.(http.Flusher)
			reader := bufio.NewReader(resp.Body)
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

		// Tổng hợp SSE thành JSON cho client non-stream
		var finalContent, reasoningContent, finalID string
		scanner := bufio.NewScanner(resp.Body)
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
					} `json:"delta"`
				} `json:"choices"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(text, "data: ")), &chunk); err == nil {
				if chunk.ID != "" {
					finalID = chunk.ID
				}
				if len(chunk.Choices) > 0 {
					finalContent += chunk.Choices[0].Delta.Content
					reasoningContent += chunk.Choices[0].Delta.ReasoningContent
				}
			}
		}

		outJSON, _ := json.Marshal(map[string]any{
			"id":      finalID,
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   targetModel,
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":              "assistant",
					"content":           finalContent,
					"reasoning_content": reasoningContent,
				},
				"finish_reason": "stop",
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
	upReq, err := http.NewRequestWithContext(r.Context(), "POST", upstreamURL, bytes.NewReader(bodyBytes))
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	sesID := newSessionID()
	upReq.Header.Set("Authorization", "Bearer public")
	upReq.Header.Set("Content-Type", "application/json")
	upReq.Header.Set("User-Agent", "opencode/1.19.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14")
	upReq.Header.Set("x-opencode-client", "cli")
	upReq.Header.Set("x-opencode-project", projectID)
	upReq.Header.Set("x-opencode-request", newRequestID())
	upReq.Header.Set("x-opencode-session", sesID)
	upReq.Header.Set("x-opencode-session-id", sesID)
	upReq.Header.Set("x-session-id", sesID)
	upReq.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(upReq)
	if err != nil {
		http.Error(w, "Upstream timeout", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
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

	log.Printf("Opencode Railway Gateway listening on port :%s", port)
	log.Printf("Available Pool Models: %v", freeModelPool)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
