# ANSWERS — Day 28 Track 2

Nguyễn Phương Nam · mã `2A202601952` · nhánh `ca-nhan-nam` · làm cá nhân.
Bằng chứng đi kèm ở [`evidence/`](evidence/README.md).

## 1. Trade-offs đã chọn

### 1.1 `event_headers` — bỏ hẳn `traceparent` thay vì gửi chuỗi rỗng

Khi không có trace đang hoạt động, hàm không thêm header `traceparent`. Cách còn
lại là luôn gửi header với giá trị rỗng cho "đồng nhất về hình dạng message".

Chọn bỏ hẳn vì một `traceparent` rỗng là header W3C **không hợp lệ**. Consumer
phía dưới sẽ phải phân biệt "chuỗi rỗng" với "không có", và bất kỳ thư viện OTel
nào đọc phải header đó cũng sẽ hoặc ném lỗi parse hoặc âm thầm tạo một trace mới,
làm đứt chuỗi trace đúng ở chỗ mình đang cố chứng minh. Vắng mặt là một tín hiệu
rõ ràng; rỗng thì không. Đổi lại, consumer phải xử lý trường hợp header thiếu —
chi phí đó rẻ hơn nhiều so với một trace giả.

`idempotency-key` thì ngược lại, luôn bắt buộc: thiếu nó thì bước MERGE ở IP03
không có khoá để chống trùng.

### 1.2 `dedupe_latest` — so sánh `(occurred_at, event_id)` chứ không chỉ `occurred_at`

Kafka không đảm bảo thứ tự giữa các partition, và hai sự kiện hoàn toàn có thể
mang cùng một `occurred_at` khi client gửi theo lô. Nếu chỉ so `occurred_at`, kết
quả khi hoà sẽ phụ thuộc thứ tự Kafka giao message — nghĩa là chạy lại cùng một
lô có thể ra kết quả khác. Ghép thêm `event_id` làm tie-breaker cho một thứ tự
toàn phần xác định.

Sắp kết quả theo `idempotency_key` cũng vì lý do đó: `MERGE` vào Delta phải sinh
ra cùng một transaction log ở mọi lần chạy thì `time travel` mới dùng để đối
chứng được. Cái giá là một lần `sorted()` trên tập khoá — không đáng kể so với
chi phí ghi Delta.

### 1.3 `readiness_status` — `mandatory` thắng tuyệt đối

Thứ tự ưu tiên: một probe `mandatory` fail là `not_ready` ngay, **không** cần xét
các probe còn lại. Chỉ khi phần bắt buộc sạch thì probe không bắt buộc fail mới
hạ xuống `degraded`.

Trade-off ở đây rất thật và đã lộ ra khi chạy: vLLM được khai báo `mandatory`,
nên trên stack cơ bản không có endpoint GPU, `lab28 ready` trả thẳng `not_ready`
chứ không phải `degraded` như README gợi ý ở Bước 7. Đó là hành vi đúng — nếu
hạ vLLM xuống không bắt buộc để lấy màu xanh đẹp hơn, hệ thống sẽ nhận traffic
trong khi không có khả năng sinh câu trả lời, tức là fail-open ở đúng chỗ nguy
hiểm nhất. Thà `not_ready` và bị Envoy loại khỏi vòng luân chuyển.

### 1.4 `feast_online_request` — lấy `FEATURE_REFS` từ `contracts.py`

Danh sách 4 feature không viết lại trong `integration_tasks.py` mà import từ
`contracts`. Viết lại sẽ nhanh hơn khi gõ, nhưng tạo ra hai nguồn sự thật: đổi
feature view ở Feast mà quên sửa một trong hai chỗ thì lỗi chỉ lộ ra lúc chạy
thật, dưới dạng `NOT_FOUND` khó lần. `scripts/verify_matrix.py` cũng đọc từ
contract, nên giữ một nguồn duy nhất là điều kiện để 245 check đó có ý nghĩa.

## 2. Production gaps

Đây là những chỗ nếu đưa hệ thống này lên production thật sẽ hỏng, xếp theo mức
độ nghiêm trọng. Tất cả đều quan sát được từ lần chạy này, không phải suy đoán.

### 2.1 `/ready` là endpoint đắt và không có cache — nghiêm trọng nhất

Mỗi lần gọi `/ready` là một lượt probe sống toàn bộ dependency: Kafka metadata,
MLflow registry, Qdrant, Feast, vLLM. Một luồng đã tốn p50 ≈ 509 ms. Ở 8 luồng
đồng thời, p95 chạm trần timeout 10 s và 56/200 request timeout.

Tệ hơn: khi đo thẳng vào API `:8000` bỏ qua gateway thì **84/100 request
timeout, p50 nhảy lên 10 s**. Nghĩa là rate limiter của Envoy hiện đang là thứ
duy nhất giữ cho API sống — nó shed tải chứ không phải cản trở.

Trong Kubernetes, readiness probe được gọi theo chu kỳ trên **mọi** pod. Với
thiết kế hiện tại, càng scale ra nhiều pod thì tải probe càng nhân lên trên cùng
các dependency dùng chung, và hệ thống có thể tự đánh sập chính mình. Cách sửa:
tách probe sang một vòng lặp nền, `/ready` chỉ đọc verdict gần nhất (TTL 1–2 s);
đặt timeout riêng và ngắn cho từng probe; thêm circuit breaker cho vLLM.

Số liệu đầy đủ ở [`evidence/load-profile.md`](evidence/load-profile.md).

### 2.2 Timeout 3 s cho probe là quá chặt, và nó đang gây flaky

`lab28 integration` chạy các probe đồng thời với timeout 3 s. Trên máy 7.8 GiB
RAM, IP01 rơi xuống `not_ready` ở 1/3 lần chạy với lỗi
`Failed to get metadata: Local: Broker transport failure`, trong khi `lab28
topics` và `lab28 ready` chạy riêng thì luôn OK.

Một readiness check flaky nguy hiểm hơn một readiness check sai cố định: nó tạo
ra pod flapping in/out khỏi service, và làm mất niềm tin vào chính tín hiệu đó.
Cần timeout theo từng dependency, có retry với backoff, và phân biệt "probe hết
giờ" với "dependency thật sự chết".

### 2.3 MLflow chạy backend SQLite

`lab28 release` fail ngay lần đầu với
`sqlite3.IntegrityError: UNIQUE constraint failed: inputs.source_type,
inputs.source_id, ...` khi `log_model` ghi quan hệ RUN_OUTPUT → MODEL_OUTPUT.
Lần hai thành công.

SQLite không chịu được ghi đồng thời. Trong lab một người chạy thì chỉ là phiền,
nhưng registry là thứ quyết định phiên bản nào đang phục vụ — một lỗi ghi ở đây
lúc promotion nghĩa là không rollback được đúng lúc cần nhất. Production phải
dùng Postgres cho tracking store và object storage cho artifact.

### 2.4 Không có xác thực ở bất kỳ boundary nào

Envoy hiện chỉ có rate limit và `x-request-id`, không có authn/authz. Grafana còn
để `admin:admin`. Kafka chạy PLAINTEXT trên cả ba listener. Bất kỳ ai chạm được
vào mạng đều ghi được vào `data.raw`, tức là bơm được dữ liệu bẩn thẳng vào
lakehouse và vector store. Cần mTLS giữa các service, SASL cho Kafka, và JWT/OIDC
ở gateway trước khi mở ra ngoài.

### 2.5 Rate limit đặt ở tầng sai

`max_tokens=10, tokens_per_fill=10, fill_interval=1s` áp chung cho **mọi** route.
Hệ quả quan sát được: `lab28 seed --via-gateway` bị 429 trên 2 message feedback
hợp lệ, và `/health` — endpoint liveness đáng lẽ phải luôn rẻ và luôn trả lời —
ăn 99/200 mã 429 trong lúc load test. Liveness bị rate limit là cách chắc chắn để
orchestrator hiểu nhầm rằng pod đã chết và giết một pod đang khoẻ.

Cần tách limit theo route, miễn trừ `/health`, và limit theo client chứ không
theo toàn cục.

### 2.6 Alert chưa đủ để trực ca

Chỉ có 2 rule: `Lab28ApiUnavailable` và `Lab28HighErrorRatio`. Không có alert cho
Kafka consumer lag, độ tươi của feature, độ trễ p99, hay DLQ có message. Nghĩa là
đường ống có thể tụt hậu hàng giờ mà không ai biết cho tới khi người dùng than
phiền — đúng loại sự cố im lặng mà observability sinh ra để bắt.

### 2.7 Chưa chứng minh được đầu-cuối vì giới hạn phần cứng

IP02 (Airflow), IP03 (Delta) chưa chạy được, và IP10 mới chứng minh được chặng
gateway → API → Kafka. Chi tiết ở mục 4.

## 3. Đóng góp

Làm cá nhân, không chia nhóm. Một người thực hiện toàn bộ các vai trong
`docs/team-role-cards.md` theo thứ tự Bước 4 của README:

| Vai | Phạm vi | Đã làm |
|---|---|---|
| Ingestion & Orchestration | IP01–IP02 | `event_headers`; tạo topic; seed qua gateway; đọc lại `data.raw` kèm header để đối chứng. IP02 chưa chạy được. |
| Data & ML | IP03–IP04–IP06 | `dedupe_latest`, `feast_online_request`; đăng ký release và promote champion trên MLflow. IP03 chưa chạy được. |
| Serving & Retrieval | IP05–IP07 | Index 13 document vào Qdrant, truy vấn hybrid có điểm số. IP07 thiếu endpoint GPU. |
| Platform & Observability | IP08–IP10 | `readiness_status`; xác nhận 9/10 target Prometheus `up`, 2 alert rule, 1 dashboard Grafana; chụp 200/429 kèm `x-request-id`; truy vấn trace 8 span trên Jaeger. |
| Presenter | bằng chứng, demo | Gom evidence pack, viết phân tích tải và tài liệu này. |

## 4. Những gì chưa chứng minh được, và vì sao

Ghi UNVERIFIED theo đúng quy tắc của SUBMISSION.md. Không giả lập bất kỳ mục nào.

**IP02 (Airflow) và IP03 (Delta)** — cần `docker compose --profile full`, tức
thêm Airflow 3, Spark Connect và Postgres. Máy chạy lab có 7.8 GiB RAM vật lý,
Docker Desktop chỉ được cấp 3.74 GiB và lúc đo host chỉ còn 0.5 GiB trống. Đây là
tình huống README đã dự liệu ở mục Troubleshooting: *"Máy yếu hoặc treo khi chạy
toàn bộ → chuyển phần này sang máy đủ mạnh hoặc môi trường do giảng viên cung
cấp"*. Cả hai file compose đều đã `config --quiet` trả mã 0, nên phần cấu hình là
hợp lệ; chỉ thiếu phần cứng để chạy.

Do cả suite `integration-tests` bị chặn bởi một preflight chung yêu cầu Airflow ở
`:8082`, năm journey J1–J5 và ba test còn lại đều không chạy được. Bằng chứng cho
IP01, IP08, IP09, IP10 vì thế được chụp trực tiếp từ các service đang chạy, và
được đặt trong [`evidence/base-stack-live/`](evidence/base-stack-live/) với ghi
chú rõ rằng chúng **không phải** file do integration-tests sinh ra.

**IP07 (vLLM)** — chưa được cấp endpoint GPU. `ip07-vllm-identity.json` ghi
`is_real_vllm: false, detail: unreachable: ConnectError`. Không dựng server giả
OpenAI-compatible: theo Bước 9 của README, một server chỉ bắt chước OpenAI API mà
không chứng minh được `/version`, model list và metric `vllm:` thì không đạt IP07,
và theo rubric thì làm giả bị 0 điểm phần tương ứng.

**IP10 (trace)** — đạt một phần. Trace `569e755ca77915ab2531ecd7a18b548e` có 8
span đi qua `lab28-gateway` → `lab28-api` → `lab28.kafka.produce`, collector báo
499 span đã export và 0 span thất bại. Các chặng Airflow, Spark, Feast và vLLM
không có vì các thành phần đó chưa chạy.

**Bước 8 (J1–J5), failure/recovery, GitOps drift/rollback** — phụ thuộc profile
`full`, chưa chạy được. Riêng phần manifest tĩnh thì
`scripts/validate_manifests.py` đã trả mã 0.

## 5. Để hoàn tất phần còn lại

Trên máy có ≥ 16 GiB RAM, hoặc môi trường giảng viên cấp:

```text
docker compose --env-file ports.template --profile full up -d --build --wait
uv run pytest integration-tests -m "not gpu and not langsmith" -q
uv run lab28 evidence
```

Với vLLM: cấu hình endpoint theo `KAGGLE_GPU_EXTENSION.md` rồi chạy lại
`uv run lab28 ready` — trạng thái sẽ chuyển từ `not_ready` sang `ready`.
