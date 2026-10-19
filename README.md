# Opencode Railway Gateway (High-Performance WARP & Auto-Failover Edition)

Cổng API hiệu năng cao viết bằng **Go thuần (Zero Dependencies, Standard Library)** đưa free tier của `opencode.ai` ra thành API quen thuộc, tối ưu đặc biệt cho **Claude Code**, OpenAI SDK, và TypeSafe System One:

| Endpoint | Giao thức | Dành cho |
|---|---|---|
| `POST /v1/messages` (+ `/v1/messages/count_tokens`) | **Anthropic Messages** | **Claude Code**, SDK Anthropic (stream SSE, non-stream, tool_use, thinking/reasoning blocks, prompt cache) |
| `POST /v1/chat/completions` | **OpenAI Chat** | Client OpenAI (Codex, Cursor, Roo Code, SDK...), stream SSE và non-stream, tool calling |
| `POST /v1/responses` | **OpenAI Responses** | OpenAI Responses API (`instructions`, function call, event stream chuẩn) |
| `POST /v1/systemone` | **TypeSafe System One** | Đánh giá System One (`jev-1.13`) |
| `GET /v1/models`, `/v1/models/{id}` | **Model Catalog** | Danh sách model (hợp lệ đồng thời cả định dạng OpenAI lẫn Anthropic) |
| `GET /health` | Healthcheck | Kiểm tra trạng thái gateway, không cần xác thực |
| `GET /status` | Trạng thái Egress | Thông số từng egress: ready, inflight, requests, ok, 429, cooldown theo từng model |

Model hỗ trợ: `deepseek` (alias của `deepseek-v4.1-flash-free`, khuyên dùng cho coding), `nemotron` (`nemotron-3.5-lightning-free`), `big-pickle`, `mimo` (`mimo-v2.6-flash-free`), `beam` (`beam-free`), `space-bunny`, `longcat` (`longcat-2.5-preview-free`), `glm` (`glm-5.3-flash-free`), `jev-1.13`, và `auto-free` (xoay vòng 8 lane).

---

## 1. Kiến trúc Egress thế hệ mới: Cloudflare WARP (Thay thế Tor)

Ở các phiên bản trước, gateway sử dụng cụm Tor multi-daemon. Tuy nhiên, toàn bộ mạng Tor toàn cầu chỉ có khoảng ~2.000 exit node công khai bị hàng nghìn bot cào dữ liệu trên thế giới dùng chung 24/7, dẫn đến việc các IP Tor liên tục bị cạn hạn mức ngày của OpenCode (`429 FreeUsageLimitError`).

**Kiến trúc mới chuyển hẳn sang Cloudflare WARP:**

```
                  ┌────────────────────────────────────────────────────────┐
                  │              Opencode Railway Gateway                  │
                  │                    (orw-prod)                          │
                  └───────┬────────────────────────────────────────┬───────┘
                          │                                        │
           (1) Ưu tiên    │                                        │ (2) Tự động chuyển
              Direct IP   │                                        │    khi Direct 429
                          ▼                                        ▼
                  ┌───────────────┐                        ┌───────────────┐
                  │ Direct Network│                        │ Cloudflare    │
                  │   (Host IP)   │                        │ WARP SOCKS5   │
                  └───────┬───────┘                        │  (warp-proxy) │
                          │                                └───────┬───────┘
                          │                                        │
                          │   Latency ~1.5s                        │   Latency ~1.8s - 2.5s
                          │   Hạn mức: ~470 req / 2 phút           │   IP Anycast sạch, không bị cờ đen
                          ▼                                        ▼
                  ┌────────────────────────────────────────────────────────┐
                  │          Upstream: opencode.ai/zen/v1                  │
                  └────────────────────────────────────────────────────────┘
```

1. **Direct IP làm đường chính:** Khi IP máy còn hạn mức, mọi request đi thẳng bằng Direct IP với độ trễ thấp nhất (**~1.5s / request**, 0 lần retry).
2. **Cloudflare WARP làm đường dự phòng siêu tốc:**
   - Hoàn toàn **FREE 100%**, không giới hạn băng thông từ Cloudflare.
   - Chạy trên container `caomingjun/warp:latest` mở cổng SOCKS5 `1080`.
   - Sử dụng dải IP Anycast chính chủ của Cloudflare: **độ trễ siêu thấp (~20ms–50ms)**, uy tín cao, không bao giờ bị Cloudflare WAF chặn 403 như Tor.
   - Tiết kiệm RAM: Toàn bộ stack WARP chỉ tốn **~30MB RAM** (so với Tor tốn hơn 600MB).
3. **Cơ chế Cooldown 2 phút (`DIRECT_COOLDOWN=2m`):**
   - Khi Direct IP chạm ngưỡng 429 / 403 (cửa sổ trượt 2 phút của OpenCode), gateway cho Direct IP nghỉ đúng 2 phút để hồi phục 100% quota.
   - Trong suốt 2 phút này, toàn bộ request **vào thẳng WARP ngay lập tức**, không bị nghẽn hay gián đoạn.
4. **Tự động chuyển Model khi cạn hạn ngạch (`AUTO_MODEL_FALLBACK=true`):**
   - Nếu model chính (`deepseek`) bị 429 liên tiếp 3 lần trên mọi đường ra, gateway **tự động failover ngay trong cùng request** sang các model free khác trong pool: `nemotron` $\rightarrow$ `big-pickle` $\rightarrow$ `mimo` $\rightarrow$ `beam` $\rightarrow$ `space-bunny` $\rightarrow$ `longcat` $\rightarrow$ `glm`. Client Claude Code hoàn toàn không thấy lỗi.
5. **Hỗ trợ Proxy Dân cư Xoay IP (`PROXY_REFRESH_URL`):**
   - Hỗ trợ cắm proxy xoay Việt Nam (`proxyxoay.shop`, TMProxy, TinProxy...) qua `PROXY_POOL` và `PROXY_REFRESH_URL`. Khi dính 429, gateway tự động gọi GET vào link API đổi IP và chờ 3s warmup.

---

## 2. Triển khai 1-Click với Docker Compose (WARP Edition)

File `docker-compose.warp.yml` đã được cấu hình sẵn chuẩn production:

```bash
cd /home/minhdn3/Downloads/opencode-railway

# Khởi động cụm WARP + Gateway
docker compose -f docker-compose.warp.yml up -d

# Xem log hoạt động thời gian thực
docker compose -f docker-compose.warp.yml logs -f gateway
```

Lệnh dừng hoặc khởi động lại:
```bash
docker compose -f docker-compose.warp.yml restart
docker compose -f docker-compose.warp.yml down
```

---

## 3. Cấu hình biến môi trường

| Biến môi trường | Mặc định | Ý nghĩa |
|---|---|---|
| `PORT` | `8080` | Cổng lắng nghe nội bộ của Gateway (map ra `8090` trên host) |
| `DISABLE_TOR` | `true` (trong compose) | Tắt hoàn toàn cụm Tor để tiết kiệm RAM và triệt tiêu độ trễ |
| `PROXY_POOL` | `socks5://warp:1080` | Danh sách proxy ngoài (HTTP/SOCKS5), phân tách bằng dấu phẩy |
| `PREFER_DIRECT` | `true` | `true`: dùng Direct IP trước, chuyển sang Proxy/WARP khi bị 429 |
| `DIRECT_COOLDOWN` | `2m` | Thời gian Direct nghỉ khi bị 429/403 (khớp với cửa sổ trượt 2 phút upstream) |
| `AUTO_MODEL_FALLBACK` | `true` | Tự động chuyển sang model free khác khi model hiện tại bị 429 |
| `MAX_WAIT` | `300s` | Tổng ngân sách thời gian tối đa cho 1 request (tránh timeout prompt lớn) |
| `MAX_ATTEMPTS` | `30` | Số lần thử lại tối đa qua các egress/model trước khi trả lỗi |
| `DEFAULT_MODEL` | `deepseek` | Model mặc định khi client gửi tên `claude-*` |
| `PROXY_REFRESH_URL` | trống | URL gọi GET đổi IP cho proxy xoay dân cư khi bị 429 (vd: `proxyxoay.shop`) |
| `PROXY_WARMUP` | `3s` | Thời gian chờ sau khi đổi IP proxy dân cư trước khi gửi request |
| `ALLOWED_IPS` | trống | IP/CIDR client được phép gọi (để trống = chỉ loopback & LAN an toàn) |
| `RATE_LIMIT_RPM` | `6000` | Giới hạn số request/phút theo IP client |
| `MAX_CONCURRENCY` | `1000` | Số lượng request đồng thời tối đa |
| `STREAM_IDLE_TIMEOUT` | `180s` | Hủy stream upstream nếu im lặng quá lâu giữa chừng |

---

## 4. Tích hợp với Claude Code (`claude-orw`)

Gateway cung cấp endpoint `/v1/messages` tương thích 100% với đặc tả Anthropic Messages API.

Cấu hình trong `~/.claude-orw/settings.json`:

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "http://127.0.0.1:8090",
    "ANTHROPIC_API_KEY": "local",
    "ANTHROPIC_MODEL": "deepseek[1m]",
    "ANTHROPIC_SMALL_FAST_MODEL": "deepseek[1m]",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "deepseek[1m]",
    "CLAUDE_CODE_DISABLE_UNKNOWN_MODEL_WINDOW_ENFORCEMENT": "1",
    "CLAUDE_CODE_AUTO_COMPACT_WINDOW": "600000",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"
  },
  "model": "deepseek[1m]"
}
```

Chạy thử nghiệm headless:
```bash
ORW_PORT=8090 claude-orw -p "Hi, reply with: all systems operational" --output-format json </dev/null
```

---

## 5. Tương thích OpenAI (`/v1/chat/completions`)

Dùng trực tiếp với Cursor, Codex, Continue, hoặc `curl`:

```bash
curl -X POST http://127.0.0.1:8090/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek",
    "messages": [{"role": "user", "content": "Write a quicksort in Go."}],
    "stream": true
  }'
```

---

## 6. Kịch bản tự phục hồi (Self-Healing Scenarios)

| Tình huống sự cố | Hành vi tự phục hồi của Gateway |
|---|---|
| Direct IP chạm ngưỡng 429/403 | Direct vào cooldown 2 phút; request hiện tại và các request sau **chuyển thẳng sang WARP ngay lập tức** |
| Model chính (`deepseek`) cạn quota | Sau 3 lần 429 liên tiếp, gateway **tự động failover sang model tiếp theo** trong pool (`nemotron`, `mimo`, `big-pickle`...) |
| Prompt siêu lớn (>100KB context) | TTFB timeout tự động giãn lên tới **60 giây** (ngân sách tổng `MAX_WAIT=300s`), không bao giờ bị ngắt sớm |
| Header Billing / `<total_tokens>` | Tự động loại bỏ thẻ rác trong system prompt để **bảo toàn 100% cache prefix (KV Cache hit ~90%+)** |
| Quá tải số lượng đồng thời | Xếp hàng bộ nhớ đệm tối đa `QUEUE_WAIT` (30s) trước khi trả `503 Service Unavailable` |

---

## 7. Kiểm thử đơn vị & Race Detection

```bash
# Chạy toàn bộ test suite có kiểm tra data race
go test -race ./...
```
*(Bao gồm các test kịch bản tự phục hồi: `TestScenarioDirect429FailsOverToTorSeamlessly`, `TestScenarioFreeTierJSONBlockedExit`, `TestProxyRefreshURLInvokedOn429`...)*
