# Opencode Railway Gateway

Cổng API viết bằng **Go thuần (không thư viện ngoài)** đưa free tier của `opencode.ai` ra thành API quen thuộc:

| Endpoint | Dành cho |
|---|---|
| `POST /v1/messages` (+ `/v1/messages/count_tokens`) | **Claude Code**, SDK Anthropic (stream và non-stream, tool_use, thinking) |
| `POST /v1/chat/completions` | Client OpenAI (Codex, Cursor, SDK...), stream và non-stream, tool calling |
| `POST /v1/systemone` | TypeSafe System One (`jev-1.13`) |
| `GET /v1/models`, `/v1/models/{id}` | Danh sách model (đúng cả định dạng OpenAI lẫn Anthropic) |
| `GET /health` | Healthcheck, không cần xác thực |
| `GET /status` | Trạng thái từng egress: sẵn sàng, inflight, số request/429, cooldown theo model |

Model dùng được: `deepseek` (alias của `deepseek-v4.1-flash`, khuyên dùng cho coding), `nemotron`, `big-pickle`, `cat-jam`, `space-bunny`, `mimo`, `step-5`, `longcat`, `jev-1.13`, và `auto-free` (xoay vòng 7 lane).

## 1. Cách hoạt động

### Egress: Tor nhiều identity là đường chính, direct dự phòng

Hạn mức free tier của upstream tính theo **IP thoát ra × model** (không phụ thuộc session/device ID), là cửa sổ trượt (~470 request/2 phút/model trên 1 IP; IP bị 429 hồi phục sau 1–2 phút). Prompt cache của upstream theo **prefix** chứ không theo IP/session (đã đo: đổi session và đổi exit vẫn `cached=4608/4668`), nên gateway chỉ cần chia tải theo độ trễ, không cần gắn hội thoại với egress. Gateway dựa vào đó để chịu tải lớn:

1. Gateway tự chạy và giám sát các **daemon Tor**. Mỗi daemon có `TOR_IDENTITIES` (mặc định 16) **identity**: mỗi identity có username SOCKS riêng nên có circuit/exit IP riêng, tức hạn mức riêng. Dung lượng = số daemon × số identity × ~470 req/2 phút.
2. Tor đơn luồng nên 1 daemon chỉ chịu tốt ~25 request đồng thời. Gateway chạy sẵn `TOR_WARM` (6) daemon, **tự thêm daemon** khi tải trung bình vượt `TOR_LOAD` (25) mỗi daemon, tới `TOR_MAX` (10); daemon nhàn rỗi quá `TOR_IDLE_STOP` thì tắt (luôn giữ tối thiểu `TOR_WARM` để không bị cold start). Mỗi identity được dựng sẵn circuit + kết nối TLS lúc khởi động nên request đầu không phải chờ.
3. Chọn identity theo **ttfb trung bình trượt × tải hiện tại** kèm **Daemon-Aware Picking** (khi 1 daemon bị 429, retry sẽ tự động nhảy sang daemon khác để tránh dồn vào cùng 1 exit). Identity có circuit chậm hơn hẳn trung vị (>2,5×) tự bị đổi exit.
4. Identity bị 429 thì đổi circuit tức thì (đổi username SOCKS, không cần `NEWNYM`) và request được thử lại ngay trên identity của daemon khác; client không thấy lỗi.
5. **Direct** chỉ được dùng khi Tor chưa sẵn sàng hoặc mọi identity đều đang nghỉ (đường dự phòng, không để client chờ). Đặt `PREFER_DIRECT=true` để đảo ngược: direct trước (nhanh nhất khi IP còn hạn mức), Tor khi bị 429.
6. Hết đường thì chờ (tối đa `MAX_WAIT`); nếu không còn gì để chờ thì trả `429` kèm `Retry-After` ngay, không để client treo.
7. **Ma trận Exit Nodes 3 cấp**:
   - **Châu Á (`idx%3 == 1`)**: `{sg},{jp},{hk},{kr}` (ping thấp nhất ~1.5s).
   - **Tây Âu (`idx%3 == 2`)**: `{de},{nl},{fr},{gb},{ch}` (bể exit IP gigabit khổng lồ, rất ít khi 429, ~2.5s).
   - **Toàn cầu (`idx%3 == 0`)**: Exit toàn cầu làm nguồn IP dự phòng khi các nhóm quốc gia bị cạn hạn mức.

Đo thực tế (deepseek, exit châu Á, 2026-10-16): 3.000 request ở 300 đồng thời → 100% thành công, ~43 req/s, ttfb p50 ≈ 3,5 s (p95 9 s), 524 lần 429 được hấp thụ bằng đổi identity, tự mở rộng lên 5 daemon, RAM ~540 MB. Với 100 đồng thời: ttfb p50 ≈ 1,5 s.

### Fingerprint và session ID

Upstream chỉ nhận request trông như client `opencode` (header `x-opencode-*`, `User-Agent`, đủ 4 tool `bash/glob/grep/read`, `stream:true`, session ID đúng hình dạng `ses_` + 12 hex + 14 ký tự). Gateway tự bơm phần còn thiếu, kể cả tool "stub" cho client không có.

- **Quota không phụ thuộc session/project(device) ID.** Đã thử cả 4 tổ hợp đổi/giữ ID trên IP đã hết quota: đều 429 với cùng bộ đếm ngược. Việc đổi ID chỉ ảnh hưởng cache.
- Mặc định (`SESSION_ID_MODE=client`) mỗi hội thoại của client có 1 session/project ID ổn định (suy từ `X-Claude-Code-Session-Id`, `metadata.user_id.session_id`, `X-Session-Id` hoặc `user`), không biết hội thoại thì dùng ID mới cho từng request. `fresh` = luôn ID mới, `stable` = 1 ID cho cả tiến trình.

### Tương thích Claude Code (`/v1/messages`)

- Dịch Anthropic ⇄ OpenAI: system, `tool_use`/`tool_result` (kể cả song song), thinking (`reasoning_content` ⇄ block `thinking`), `stop_reason`, usage (`cache_read_input_tokens` lấy từ cache upstream).
- Tên model của Claude Code được chuẩn hóa: `deepseek[1m]` → `deepseek`, mọi tên `claude-*` → `DEFAULT_MODEL` (mặc định `deepseek`).
- Tool stub bơm vào có mô tả "đừng gọi"; nếu model vẫn gọi, gateway dịch ngược về tool của client (`bash→Bash`, `read→Read` kèm `filePath→file_path`...).
- Bỏ block `x-anthropic-billing-header` và thẻ `<total_tokens>...` trong system prompt (thay đổi mỗi request, làm hỏng cache prefix); bỏ `tool_use` không có kết quả và kết quả mồ côi; ảnh được hạ thành `[image attached]` với model chỉ nhận text (đặt `IMAGE_MODELS` cho model có vision).
- Stream đứt giữa chừng được báo bằng `event: error` (client tự retry), không giả làm câu trả lời hoàn chỉnh. Timeout TTFB tự động thích ứng theo độ dài payload (lên tới 45s cho prompt >100KB như omp context lớn), tránh ngắt sớm gây 529.
- Chưa hỗ trợ: server tool của Anthropic (`web_search`, `web_fetch` chạy phía server) bị bỏ qua; ảnh trong `tool_result` chỉ còn ghi chú văn bản.

## 2. Xác thực theo IP

Không có API key; quyền truy cập dựa trên **IP client**:

- `ALLOWED_IPS` = danh sách IP/CIDR phân tách bằng dấu phẩy (ví dụ `203.0.113.5,10.0.0.0/8`). `*` = mở hoàn toàn. **Để trống = chỉ loopback và mạng nội bộ** (an toàn mặc định).
- `RATE_LIMIT_RPM` (mặc định 6000) và `RATE_LIMIT_BURST` (mặc định 600): giới hạn theo IP cho các endpoint POST; vượt thì `429` + `Retry-After`.
- Phía sau proxy (Railway) `RemoteAddr` là proxy nội bộ nên IP thật lấy từ header: `CLIENT_IP_HEADER` (mặc định `X-Real-IP` khi có `RAILWAY_ENVIRONMENT`; với `X-Forwarded-For` lấy phần tử ngoài cùng bên phải). **Chỉ đặt khi gateway thật sự đứng sau proxy tin cậy**, nếu không client có thể tự khai IP. Bị từ chối thì log ghi `[Auth] DENY ip=... xff=... real-ip=...` để biết header nào mang IP thật.
- CORS mặc định tắt; đặt `CORS_ORIGIN` nếu cần gọi từ trình duyệt.

## 3. Deploy lên Railway

```bash
railway up      # hoặc nối repo GitHub, Railway tự build Dockerfile
```

Biến nên đặt: `ALLOWED_IPS=<IP máy bạn>`, `TOR_WARM=1 TOR_MAX=2 TOR_IDENTITIES=4` (mỗi daemon Tor chiếm khoảng 100 MB RAM; gói 512 MB nên giữ nhỏ). Railway dùng `PORT` tự động. Healthcheck path: `/health`.

## 4. Biến môi trường

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `PORT` | `8080` | Cổng lắng nghe |
| `ALLOWED_IPS` | trống | IP/CIDR được phép (xem mục 2) |
| `CLIENT_IP_HEADER` | tự động trên Railway | Header mang IP client thật |
| `RATE_LIMIT_RPM` / `RATE_LIMIT_BURST` | `6000` / `600` | Rate limit theo IP (`RPM=0` tắt) |
| `CORS_ORIGIN` | trống (tắt) | Origin được phép CORS |
| `MAX_CONCURRENCY` | `1000` | Số request đồng thời tối đa |
| `MAX_BODY_MB` | `32` | Kích thước body tối đa |
| `DISABLE_TOR` | `false` | Chỉ dùng direct |
| `FORCE_TOR` | `false` | Không dùng direct (để đo/so sánh) |
| `TOR_WARM` | `6` | Số daemon Tor chạy sẵn lúc khởi động |
| `TOR_IDENTITIES` | `16` | Số identity (exit IP riêng) mỗi daemon |
| `TOR_LOAD` | `25` | Request đồng thời/daemon, vượt thì tự bật thêm daemon |
| `TOR_EXIT_NODES` | `{sg},{jp},{hk},{kr}` | Giới hạn quốc gia exit châu Á |
| `TOR_EXIT_NODES_EU` | `{de},{nl},{fr},{gb},{ch}` | Giới hạn quốc gia exit Tây Âu |
| `TTFB_RETRY` | `15s` | Tor chưa có header sau thời gian này thì đổi circuit |
| `PREFER_DIRECT` | `false` | `true`: direct trước, Tor khi 429 |
| `TOR_IDLE_STOP` | `30m` | Tor nhàn rỗi quá lâu thì tắt |
| `TOR_MAX_INFLIGHT` | `24` | Trần request đồng thời mỗi identity |
| `DIRECT_WARN_REQS` | `150` | (PREFER_DIRECT) request direct/model trong 2 phút được coi là gần ngưỡng nguy hiểm |
| `DIRECT_WARN_INFLIGHT` | `60` | Request direct đồng thời được coi là nguy hiểm |
| `QUEUE_WAIT` | `30s` | Chờ slot tối đa trước khi trả 503 |
| `TOR_MAX` (hoặc `TOR_COUNT`) | `10` | Số daemon Tor tối đa (tối ưu ngân sách 3GB RAM) |
| `TOR_BIN` | `tor` | Đường dẫn binary Tor |
| `TOR_BACKOFF_MAX` | `2m` | Trần giãn nhịp của Tor sau chuỗi 429 |
| `TOR_INSTANCES` | trống | Tor ngoài có sẵn, dạng `host:socks,host:socks` |
| `PROXY_POOL` | trống | Proxy ngoài (URL `socks5://`, `http://`), cách nhau bằng dấu phẩy |
| `COOLDOWN_MAX` | `2m` | Trần cooldown của direct/proxy tĩnh |
| `MAX_WAIT` / `MAX_ATTEMPTS` | `90s` / `30` | Chờ tối đa / số lần thử mỗi request |
| `STREAM_IDLE_TIMEOUT` | `180s` | Hủy stream upstream im lặng quá lâu |
| `SESSION_ID_MODE` / `PROJECT_ID_MODE` | `client` | `client`, `fresh` hoặc `stable` |
| `DEFAULT_MODEL` | `deepseek` | Model dùng cho tên `claude-*` ở `/v1/messages` |
| `IMAGE_MODELS` | trống | Danh sách model nhận ảnh |
| `INJECT_TOOLS_ANTHROPIC` | `true` | Bơm 4 tool stub ở `/v1/messages` (upstream bắt buộc) |

## 5. Test với Claude Code

Tạo profile riêng, không dùng chung cấu hình với bản Claude Code đang có. Trong `~/.claude-orw/settings.json`:

```json
{
  "env": {
    "ANTHROPIC_API_KEY": "local",
    "ANTHROPIC_MODEL": "deepseek[1m]",
    "ANTHROPIC_SMALL_FAST_MODEL": "deepseek[1m]",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "deepseek[1m]"
  },
  "model": "deepseek[1m]"
}
```

Launcher chạy với `CLAUDE_CONFIG_DIR=~/.claude-orw` và `ANTHROPIC_BASE_URL=http://127.0.0.1:<cổng>`. **Không đặt `ANTHROPIC_BASE_URL` trong `settings.json`**: khóa trong settings thắng biến môi trường nên sẽ làm launcher không đổi được cổng.

Đo so sánh direct / Tor / hybrid bằng cùng một task multi-turn: `scripts/bench_claude.sh deepseek 3`.

Test tự động: `go test -race ./...` (upstream giả, kiểm tra auth, dịch Anthropic, stream, định tuyến direct→Tor, backoff, sticky).

## 6. Giới hạn free tier (đo thực tế 2026-10-15/16)

- **Quota theo IP × model, cửa sổ trượt.** Khi hết, `429 FreeUsageLimitError` (có thể kèm `Retry-After`); gateway đặt cooldown theo đó nhưng chặn trần bằng `COOLDOWN_MAX` vì IP hồi phục sớm hơn con số báo.
- **IP direct dùng chung hạn mức với mọi dịch vụ khác chạy trên cùng IP** (ví dụ một gateway khác trên cùng máy).
- **Exit Tor là tài nguyên dùng chung của mọi người dùng Tor**; vào những ngày nhiều người dùng free tier, phần lớn exit đã cạn quota nên Tor chỉ là dự phòng không chắc chắn: có thể thử nhiều circuit mới trúng một exit còn quota. Cần ổn định thì dùng proxy xoay trả phí qua `PROXY_POOL`.
- Qua Tor, thời gian tới byte đầu tiên khoảng 1–4 giây (direct khoảng 0,7–1,5 giây).

## 7. Lưu ý sử dụng

Gateway giả danh client `opencode` và đổi egress để né giới hạn theo IP của free tier. Việc này có thể vi phạm điều khoản của nhà cung cấp và Tor exit là IP dùng chung. Chỉ dùng khi bạn được phép và chịu trách nhiệm.

## Số đo direct vs Tor (deepseek, task 4 turn qua Claude Code, 2026-10-16)

| Chế độ | Thời gian task | Ghi chú |
|---|---|---|
| direct | 9–11 s | ttfb thường 0,7–2,7 s |
| tor (2 instance) | 11–23 s | ttfb thường 1,8–6 s; 0 lỗi 429 lúc đo |
| hybrid (direct trước) | 17–25 s | cả 9 request đều đi direct, không cần Tor |

Direct nhanh hơn khoảng 1,5–2 lần nên mặc định là direct trước, Tor chỉ làm đường dự phòng. `opencode2api` (dự án tham khảo) cũng đổi IP, chỉ khác là nó dùng 9 daemon Tor × 144 identity xoay vòng và bỏ identity vừa bị 429 ngay.

### Sức chịu tải đo được (deepseek, 1 IP direct, tải nhỏ ~470 token prompt)

- Direct chịu tới khoảng **470 request / 2 phút** cho 1 model rồi bị 429 (cửa sổ trượt); với 100 request đồng thời đạt ~30 req/s, ttfb p50 ≈ 1,5 s.
- Sau 429, IP direct hồi phục trong khoảng **1–2 phút** (dù `Retry-After` báo ~22 giờ), nên gateway bỏ qua Retry-After dài và thăm dò lại sau 15s/30s/1m bằng đúng 1 request.
- Khi vượt trần, gateway chuyển sang Tor (dựng 2 Tor song song ngay khi direct 429, hoặc bật sẵn từ `DIRECT_WARN_REQS`). Một đợt 700 request/150 đồng thời vượt trần: 700/700 thành công, không lỗi phía client, nhưng đuôi trễ p95 ≈ 33 s do chờ Tor bootstrap.
- Dồn liên tục hơn ~2.000 request trong vài phút làm cạn cả direct lẫn nhiều exit Tor: một phần request nhận 429 kèm `Retry-After` (không treo). Muốn tải bền vững cao hơn cần thêm IP thoát (proxy tĩnh `PROXY_POOL`) hoặc dùng nhiều model.

## Endpoint hỗ trợ

`POST /v1/messages` (Anthropic, stream + không stream, tool use, thinking, ảnh, `count_tokens`), `POST /v1/chat/completions` (OpenAI), `POST /v1/responses` (OpenAI Responses: input chuỗi/mảng, `instructions`, function tool, stream đủ sự kiện — dùng được với Codex), `POST /v1/completions` (cũ; model có reasoning cần `max_tokens` đủ lớn vì phần suy luận cũng tính vào), `GET /v1/models[/{id}]`, `/health`, `/status`. Đường dẫn lạ trả JSON `404`, thiếu `messages` trả `400`.

## Kịch bản lỗi đã kiểm thử

Mỗi kịch bản có test tự động (`scenario_test.go`, upstream giả) và những kịch bản đánh dấu (live) đã chạy thật:

| Kịch bản | Hành vi |
|---|---|
| 1 identity bị 429 | Đổi circuit tức thì, thử lại trên identity khác; client không thấy lỗi |
| Mọi identity 429 | Chờ/thử trong `MAX_WAIT` (tổng ngân sách mỗi request), rồi trả `429` + `Retry-After`; không bao giờ treo, không vượt `MAX_ATTEMPTS` |
| 429 hàng loạt cùng lúc | Tính 1 lần (không bùng nổ backoff); 120 request đồng thời cùng gặp 429 vẫn thành công hết |
| 502/503/504, lỗi mạng | Identity nghỉ ngắn, thử identity khác |
| Circuit Tor kẹt (không có header) | Sau `TTFB_RETRY` bỏ circuit đó, thử identity khác |
| Cloudflare chặn exit (403 HTML) | Đổi exit, thử lại |
| Identity sau cooldown | Được dùng lại (không bị loại vĩnh viễn) |
| Daemon Tor bị kill giữa tải (live: kill 2 daemon khi 40 đồng thời, 400 request) | 400/400 thành công; daemon tự khởi động lại, autoscale bù |
| Client bỏ giữa chừng | Huỷ request upstream, trả slot/inflight |
| Upstream cắt stream giữa chừng | Client nhận `event: error` rõ ràng |
| Stream im lặng | Watchdog `STREAM_IDLE_TIMEOUT` |
| Quá `MAX_CONCURRENCY` | Xếp hàng tối đa `QUEUE_WAIT` rồi `503` |
