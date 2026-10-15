# Opencode Railway Gateway (Ultra-Fast & Zero-Proxy)

Cổng API Gateway siêu nhẹ viết bằng **Go 1.22 thuần (Standard Library, zero dependencies)**, được tối ưu hóa đặc biệt để **deploy lên Railway chỉ với 1 click**.

---

## 1. Tính năng nổi bật

1. **Chế độ `auto-free` (Cụm 5 Worker Lanes)**:
   - Tự động chia tải vòng tròn (Round-Robin) qua 5 model free hàng đầu: `nemotron-3.5-lightning`, `big-pickle`, `cat-jam`, `space-bunny`, `mimo-v2.6-flash`.
   - **Tự động né và xoay khi 429 (Circuit Breaker)**: Nếu bất kỳ model nào bị dính 429, gateway tự động ngắt mạch model đó trong 15s và lập tức chuyển request sang model khác với Session ID mới tinh $\rightarrow$ Client luôn nhận về `200 OK`.
2. **Hỗ trợ gọi lẻ từng Model**:
   - Tự do gọi đích danh từng model (`nemotron-3.5-lightning`, `big-pickle`, `cat-jam`, `space-bunny`, `mimo-v2.6-flash`).
   - Bảo toàn 100% tham số client: `temperature`, `max_tokens`, `thinking` budget, `reasoning_effort`, `stream`.
3. **Hỗ trợ trọn vẹn TypeSafe System One (JEV)**:
   - Endpoint `POST /v1/systemone` phục vụ mô hình `jev-1.13` với tốc độ phân loại nhị phân / chấm điểm siêu tốc.
4. **Không cần bất kỳ Proxy nào (100% Direct IP)**:
   - Tự động sinh fingerprint OpenCode client 1.19.0 chuẩn xác (`ses_` TypeID descending timestamp, `msg_` ascending timestamp, `x-opencode-client: cli`, 4 công cụ bắt buộc `bash/glob/grep/read`).

---

## 2. Hướng dẫn Deploy lên Railway

### Cách 1: Deploy qua GitHub (Khuyên dùng)
1. Đẩy thư mục này lên một repository GitHub của bạn (ví dụ: `https://github.com/your-username/opencode-railway`).
2. Truy cập [Railway.app](https://railway.app), chọn **New Project** $\rightarrow$ **Deploy from GitHub repo**.
3. Chọn repo `opencode-railway`. Railway sẽ tự động nhận diện `Dockerfile` hoặc Go runtime và build chỉ trong 30 giây!
4. Trong mục **Settings** của project trên Railway $\rightarrow$ **Networking** $\rightarrow$ bấm **Generate Domain** để lấy URL public (ví dụ: `https://opencode-production.up.railway.app`).

### Cách 2: Deploy qua Railway CLI
```bash
npm i -g @railway/cli
railway login
cd /home/minhdn3/Downloads/opencode-railway
railway init
railway up
```

---

## 3. Hướng dẫn sử dụng API sau khi Deploy

### A. Dùng chế độ Auto (Tự động chia tải & chống 429)
* **Base URL**: `https://<your-railway-domain>/v1`
* **Model**: `"auto-free"` hoặc `"auto"`
```bash
curl -X POST https://<your-railway-domain>/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "auto-free",
    "messages": [{"role": "user", "content": "Xin chào, bạn là model nào?"}],
    "stream": true
  }'
```

### B. Dùng model lẻ (Ví dụ: Big-Pickle với Thinking sâu)
```bash
curl -X POST https://<your-railway-domain>/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "big-pickle",
    "messages": [{"role": "user", "content": "Giải bài toán tối ưu hóa thuật toán"}],
    "reasoning_effort": "high"
  }'
```

### C. Dùng JEV System One (Phân loại siêu tốc)
```bash
curl -X POST https://<your-railway-domain>/v1/systemone \
  -H "Content-Type: application/json" \
  -d '{
    "model": "jev-1.13",
    "state": {"text": "Khách hàng phàn nàn dịch vụ giao hàng chậm"},
    "questions": {
      "is_complaint": {"type": "noul", "instructions": "Đây có phải khiếu nại không?"}
    }
  }'
```

### D. Kiểm tra danh sách Models
```bash
curl https://<your-railway-domain>/v1/models
```
