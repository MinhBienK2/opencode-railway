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

### Egress: direct trước, Tor khi bị 429

Hạn mức free tier của upstream tính theo **IP thoát ra × model** (không phụ thuộc session/device ID), và là cửa sổ trượt chứ không phải mốc cố định: IP direct bị 429 hồi phục sau một thời gian không dùng (đã quan sát phục hồi giữa ngày, không cần chờ 00:00 UTC). Gateway dựa vào đó:

1. Mọi request đi **IP direct** của container trước (nhanh nhất).
2. Khi direct trả `429` cho model đó, gateway ghi cooldown cho cặp **(direct × model)** và chuyển ngay sang **Tor**. Model khác vẫn dùng direct bình thường. Cooldown theo `Retry-After` của upstream, nhưng tối đa `COOLDOWN_MAX` (mặc định 10 phút) rồi thăm dò lại bằng **đúng 1 request** (WAN kép có thể đổi IP).
3. Tor do **chính gateway chạy và giám sát** (tự khởi động lại khi chết) và **mặc định không chạy**. Tor chỉ được bật sẵn ở nền khi có dấu hiệu nguy hiểm: direct vừa bị 429, hoặc lưu lượng direct của một model đạt `DIRECT_WARN_REQS` request/2 phút, hoặc số request direct đồng thời đạt `DIRECT_WARN_INFLIGHT`. Cần thêm thì bật tới `TOR_MAX`; mỗi instance có trần `TOR_MAX_INFLIGHT` request đồng thời; nhàn rỗi quá `TOR_IDLE_STOP` thì tự tắt để nhường RAM. Tor khởi động mất 10–45 giây, nên nếu IP direct của bạn thường xuyên hết quota hãy đặt `TOR_WARM=1` để luôn có sẵn một Tor.
4. Mỗi Tor instance giữ **một circuit/exit cho tới khi bị 429**, rồi đổi sang circuit mới tức thì bằng cách đổi username SOCKS (Tor tách circuit theo `IsolateSOCKSAuth`, không cần `NEWNYM` và không bị giới hạn 10 giây). Instance từng bị 429 thì bị đẩy xuống ưu tiên sau; sau 5 lần 429 liên tiếp thì giãn dần (tối đa `TOR_BACKOFF_MAX`) để không đập vào upstream khi cả bể exit hôm đó đã cạn.
5. Hết đường thì chờ (tối đa `MAX_WAIT`); nếu không còn gì để chờ thì trả `429` kèm `Retry-After` ngay, không để client treo.

Hội thoại của client được giữ **dính vào cùng một egress Tor** khi nó còn khỏe, để prompt cache phía upstream không bị mất.

### Fingerprint và session ID

Upstream chỉ nhận request trông như client `opencode` (header `x-opencode-*`, `User-Agent`, đủ 4 tool `bash/glob/grep/read`, `stream:true`, session ID đúng hình dạng `ses_` + 12 hex + 14 ký tự). Gateway tự bơm phần còn thiếu, kể cả tool "stub" cho client không có.

- **Quota không phụ thuộc session/project(device) ID.** Đã thử cả 4 tổ hợp đổi/giữ ID trên IP đã hết quota: đều 429 với cùng bộ đếm ngược. Việc đổi ID chỉ ảnh hưởng cache.
- Mặc định (`SESSION_ID_MODE=client`) mỗi hội thoại của client có 1 session/project ID ổn định (suy từ `X-Claude-Code-Session-Id`, `metadata.user_id.session_id`, `X-Session-Id` hoặc `user`), không biết hội thoại thì dùng ID mới cho từng request. `fresh` = luôn ID mới, `stable` = 1 ID cho cả tiến trình.

### Tương thích Claude Code (`/v1/messages`)

- Dịch Anthropic ⇄ OpenAI: system, `tool_use`/`tool_result` (kể cả song song), thinking (`reasoning_content` ⇄ block `thinking`), `stop_reason`, usage (`cache_read_input_tokens` lấy từ cache upstream).
- Tên model của Claude Code được chuẩn hóa: `deepseek[1m]` → `deepseek`, mọi tên `claude-*` → `DEFAULT_MODEL` (mặc định `deepseek`).
- Tool stub bơm vào có mô tả "đừng gọi"; nếu model vẫn gọi, gateway dịch ngược về tool của client (`bash→Bash`, `read→Read` kèm `filePath→file_path`...).
- Bỏ block `x-anthropic-billing-header` trong system prompt (đổi mỗi request, làm hỏng cache prefix); bỏ `tool_use` không có kết quả và kết quả mồ côi; ảnh được hạ thành `[image attached]` với model chỉ nhận text (đặt `IMAGE_MODELS` cho model có vision).
- Stream đứt giữa chừng được báo bằng `event: error` (client tự retry), không giả làm câu trả lời hoàn chỉnh. Upstream im lặng quá `STREAM_IDLE_TIMEOUT` thì hủy để không giữ slot mãi.
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

Biến nên đặt: `ALLOWED_IPS=<IP máy bạn>`, `TOR_MAX=2` (mỗi Tor chiếm khoảng 50–100 MB RAM; gói 512 MB nên giữ `TOR_MAX` nhỏ). Railway dùng `PORT` tự động. Healthcheck path: `/health`.

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
| `TOR_WARM` | `0` | Số Tor chạy sẵn lúc khởi động (0 = chỉ bật khi direct nguy hiểm) |
| `TOR_IDLE_STOP` | `30m` | Tor nhàn rỗi quá lâu thì tắt |
| `TOR_MAX_INFLIGHT` | `24` | Trần request đồng thời mỗi Tor |
| `DIRECT_WARN_REQS` | `200` | Request direct/model trong 2 phút được coi là gần ngưỡng nguy hiểm (bật Tor sẵn) |
| `DIRECT_WARN_INFLIGHT` | `60` | Request direct đồng thời được coi là nguy hiểm |
| `QUEUE_WAIT` | `30s` | Chờ slot tối đa trước khi trả 503 |
| `TOR_MAX` (hoặc `TOR_COUNT`) | `4` | Số Tor tối đa được bật thêm khi bị 429 |
| `TOR_BIN` | `tor` | Đường dẫn binary Tor |
| `TOR_BACKOFF_MAX` | `2m` | Trần giãn nhịp của Tor sau chuỗi 429 |
| `TOR_INSTANCES` | trống | Tor ngoài có sẵn, dạng `host:socks,host:socks` |
| `PROXY_POOL` | trống | Proxy ngoài (URL `socks5://`, `http://`), cách nhau bằng dấu phẩy |
| `COOLDOWN_MAX` | `10m` | Trần cooldown của direct/proxy tĩnh |
| `MAX_WAIT` / `MAX_ATTEMPTS` | `90s` / `30` | Chờ tối đa / số lần thử mỗi request |
| `STREAM_IDLE_TIMEOUT` | `120s` | Hủy stream upstream im lặng quá lâu |
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
