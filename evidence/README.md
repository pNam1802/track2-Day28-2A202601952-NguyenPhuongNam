# Evidence pack — Day 28 Track 2

Repo: `track2-Day28-2A202601952-NguyenPhuongNam` · nhánh `ca-nhan-nam` · làm cá nhân.
Toàn bộ file dưới đây chụp từ stack chạy thật bằng `docker compose --env-file
ports.template up -d --build --wait`. Không có file nào được dựng bằng tay hay
mô phỏng. Mục nào chưa chứng minh được thì ghi UNVERIFIED kèm lý do.

## Trạng thái 10 integration point

| IP | Boundary | Trạng thái | Bằng chứng |
|---|---|---|---|
| IP01 | HTTP ingestion → Kafka | **ready** | [ip01-kafka-live.txt](base-stack-live/ip01-kafka-live.txt) |
| IP02 | Kafka → Airflow 3 | UNVERIFIED | cần profile `full` — xem [environment.txt](fast-checks/environment.txt) |
| IP03 | Airflow/Spark → Delta | UNVERIFIED | cần profile `full` (Spark Connect) |
| IP04 | Delta → Feast | **ready** (service) | [integration-report.json](integration-report.json) — endpoint healthy; dòng online sau materialize cần IP03 |
| IP05 | Delta documents → Qdrant | **ready** | [ip05-qdrant-search.json](ip05-qdrant-search.json) |
| IP06 | Evaluation → MLflow Registry | **ready** | [ip06-mlflow-release.json](ip06-mlflow-release.json) |
| IP07 | RAG prompt → real vLLM | UNVERIFIED | [ip07-vllm-identity.json](ip07-vllm-identity.json) — chưa được cấp endpoint GPU |
| IP08 | Client → Envoy gateway | **ready** | [ip08-gateway-live.txt](base-stack-live/ip08-gateway-live.txt) |
| IP09 | Components → Prometheus/Grafana | **ready** | [ip09-prometheus-grafana-live.txt](base-stack-live/ip09-prometheus-grafana-live.txt) |
| IP10 | Components → OTLP trace | **một phần** | [ip10-trace-live.txt](base-stack-live/ip10-trace-live.txt) — chặng gateway→API→Kafka; các chặng Airflow/Spark/vLLM cần profile `full` |

`lab28 integration` chấm **67/100**, 4/6 point probe được là passing, 4 point
không probe được từ tiến trình serving.

## Các file khác

| File | Nội dung |
|---|---|
| [fast-checks/fast-checks.txt](fast-checks/fast-checks.txt) | ruff + verify_matrix + check_portability + validate_manifests + 87 test passed, kèm commit SHA |
| [fast-checks/environment.txt](fast-checks/environment.txt) | cấu hình máy, ba sự cố môi trường đã xử lý, và giới hạn RAM còn lại |
| [step7-base-stack.txt](step7-base-stack.txt) | log đầy đủ Bước 7: `topics`, `index`, `release`, `seed`, `inspect`, `ready` |
| [integration-scoring.txt](integration-scoring.txt) | output `lab28 integration` |
| [integration-report.json](integration-report.json) | báo cáo máy đọc được của 10 point |
| [evidence-run.txt](evidence-run.txt) | output `lab28 evidence` |
| [load-profile.json](load-profile.json) | output đúng lệnh trong SUBMISSION.md |
| [load-profile.md](load-profile.md) | P50/P95/P99 ở nhiều mức đồng thời + phân tích nghẽn cổ chai |

## Ba điều cần nói rõ khi demo

1. **`ready` trả `not_ready`, không phải `degraded`.** vLLM là probe `mandatory`
   trong `readiness_status()`, nên thiếu endpoint GPU là cả hệ thống
   `not_ready`. Đây là hành vi đúng theo thiết kế của Phần D, không phải lỗi.

2. **IP01 flaky ở mức probe.** `lab28 integration` chạy các probe đồng thời với
   timeout 3 s; trên máy 7.8 GiB RAM, Kafka metadata thỉnh thoảng vượt ngưỡng và
   IP01 rơi xuống `not_ready` (quan sát 1/3 lần). `lab28 topics` và `lab28 ready`
   chạy riêng thì luôn OK. Đây là nghẽn tài nguyên máy, không phải lỗi contract.

3. **`lab28 release` fail lần đầu.** MLflow 3.15.2 với backend SQLite ném
   `UNIQUE constraint failed: inputs.source_type, inputs.source_id, ...` khi
   `log_model` ghi quan hệ RUN_OUTPUT → MODEL_OUTPUT. Chạy lại lần hai thành
   công. Log cả hai lần đều giữ trong `step7-base-stack.txt`.
