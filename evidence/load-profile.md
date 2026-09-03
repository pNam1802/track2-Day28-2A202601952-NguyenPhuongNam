# Load profile & bottleneck analysis

Captured on the base stack (`docker compose --env-file ports.template up -d`),
without the `full` profile and without a real vLLM endpoint.
Host: 8 vCPU / 7.8 GiB RAM, Docker Desktop limited to 3.74 GiB.

## Lệnh chuẩn theo SUBMISSION.md

```text
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

```json
{
  "requests": 200, "workers": 8,
  "status_counts": { "0": 192, "200": 8 },
  "latency_ms": { "p50": 10.08, "p95": 10002.42, "p99": 10033.57 }
}
```

`run_profile.py` gọi `urllib.request.urlopen`, mà hàm này ném exception cho **mọi**
mã không phải 2xx. Kịch bản gộp tất cả thành `status: 0`, nên con số trên không
phân biệt được 429 (rate limit), 503 (not ready) và timeout phía client. Đo lại
với biến thể ghi đúng mã HTTP để phân tích.

## Đo lại có phân biệt mã trả về

| Kịch bản | 200 | 429 | 503 | timeout | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `/ready` qua gateway, 8 luồng, 200 req | 144 | 0 | 0 | 56 | 1032 ms | 10029 ms | 10034 ms |
| `/health` qua gateway, 8 luồng, 200 req | 68 | 99 | 17 | 16 | 80 ms | 10006 ms | 10012 ms |
| `/ready` thẳng vào API `:8000`, 8 luồng, 100 req | 8 | – | 8 | 84 | 10012 ms | 10141 ms | 10216 ms |
| `/ready` qua gateway, 1 luồng, 20 req | 17 | 0 | 0 | 3 | 509 ms | 10010 ms | 10011 ms |

## Nghẽn cổ chai

**1. `/ready` là endpoint đắt và không có cache.** Mỗi lần gọi là một lượt probe
sống toàn bộ dependency: Kafka metadata, MLflow registry, Qdrant, Feast và vLLM.
Một luồng đã tốn p50 ≈ 509 ms. Tám luồng đồng thời nhân số kết nối ra ngoài lên
tám lần trên cùng một event loop, đẩy p95 chạm trần timeout 10 s. Đây là nghẽn
chính, không phải nghẽn ở tầng mạng hay gateway.

**2. Probe vLLM là phần đắt nhất trong `/ready`.** Không có endpoint GPU nên mỗi
probe phải chờ TCP connect tới `host.docker.internal:8001` thất bại rồi mới trả
verdict. Khi có vLLM thật, phần này sẽ thành một HTTP call bình thường.

**3. Rate limit của Envoy đang là cơ chế bảo vệ, không phải cản trở.** Hàng
`/ready` thẳng vào API cho thấy khi **bỏ qua** gateway thì kết quả *tệ hơn hẳn*:
84/100 timeout và p50 nhảy lên 10 s, so với 144/200 thành công khi đi qua
gateway. Envoy (`max_tokens=10`, `tokens_per_fill=10`, `fill_interval=1s`) đang
shed bớt tải, giữ cho API còn phục vụ được phần còn lại. Đó là lý do hàng
`/health` xuất hiện 99 mã 429: rate limiter cắt sớm thay vì để request xếp hàng
tới lúc timeout.

**4. 503 trên `/health` là hệ quả dây chuyền.** Envoy trả 503 khi upstream đã bão
hòa và không nhận thêm kết nối, khớp với thời điểm API đang bị `/ready` chiếm.

## Việc nên làm nếu đưa lên production

- Cache verdict của `/ready` trong 1–2 giây, hoặc tách probe sang một vòng lặp
  nền và để `/ready` chỉ đọc kết quả gần nhất. Kubernetes readiness probe gọi
  theo chu kỳ nên không cần dữ liệu tươi từng request.
- Đặt timeout riêng và ngắn cho từng probe dependency, kèm circuit breaker cho
  vLLM, để một phụ thuộc chết không kéo dài toàn bộ verdict.
- Tách `/health` (liveness, phải rẻ và luôn nhanh) khỏi `/ready` (readiness, đắt)
  ở mức route trên Envoy, và không áp cùng một rate limit cho cả hai.
- Chạy nhiều worker uvicorn; hiện tại một tiến trình phải gánh cả probe lẫn
  ingestion.
