# Processing evidence

**Bằng chứng chính là lần chạy ngày 04/11/2026**, với code cuối cùng (có `forecast` và ngưỡng theo kênh). Q1–Q4 và đối soát silver cho cùng số liệu với lần chạy ngày 29/10.

| File | Nội dung |
| --- | --- |
| `run_20261104_112206.log` | Log `./run.sh` từ `infra` đến `export`, đã lọc các dòng INFO và WARN của Spark vì bản đầy đủ nặng 14 MB |
| `ingest_20261104T042220Z.json` | Manifest ingest |
| `silver_quality_app-20261104042634-0001.json` | Báo cáo chất lượng silver, gồm outlier theo kênh |
| `forecast_report_app-20261104043422-0003.json` | Độ chính xác của mô hình, mức độ quan trọng của đặc trưng, tổng dự báo tuần 10–11 |

Application ID: bronze `app-20261104042345-0000`, silver `app-20261104042634-0001`, gold `app-20261104043142-0002`, forecast `app-20261104043422-0003`, export `app-20261104044237-0004`.

## Lần chạy ngày 29/10/2026

Lần chạy trước khi có `forecast`, trên máy khác, mất 7 phút 16 giây.

| File | Nội dung |
| --- | --- |
| `run_20261029_205823.log` | Log đầy đủ `./run.sh` từ `infra` đến `export`, tổng thời gian 7 phút 16 giây |
| `ingest_20261029T135919Z.json` | Manifest ingest: số byte, số dòng, SHA-256 của 5 file CSV |
| `silver_quality_app-20261029140124-0001.json` | Báo cáo chất lượng silver: 74,180,464 dòng bronze = 74,180,464 fact + 0 rejected, `invariant_ok: true` |

Spark application ID của lần chạy này (xem lại trên History Server, http://localhost:18080):

| Job | Application ID | Kết quả |
| --- | --- | --- |
| bronze | `app-20261029140008-0000` | `train`: 74,180,464 dòng |
| silver | `app-20261029140124-0001` | fact + rejected = bronze |
| gold | `app-20261029140355-0002` | Q1 1,393 · Q2 1,062,353 · Q3 17,604 · Q4 880,604 dòng |
| export | `app-20261029140518-0003` | 4 bảng `gold.*` trong PostgreSQL |

## Lần chạy cũ (Checkpoint 2, ngày 19/10/2026)

`run_20261019_162611.log`, `ingest_20261019T092615Z.json` và `silver_quality_app-20261019092920-0005.json` được giữ lại làm lịch sử Checkpoint 2. Ở lần chạy đó, `gold.py` mới chỉ có Q1, nên log ghi Q2–Q4 là `not implemented yet, skipped`. Không dùng lần chạy này làm bằng chứng cho kết quả cuối cùng.

## EDA (ngày 04/11/2026)

`eda_run_20261104.log` là log của job `spark_jobs/eda.py`, Spark application `app-20261104033847-0002`. Job chỉ đọc bronze, silver, gold và không thay đổi kết quả của pipeline. Báo cáo: `docs/eda/EDA_report.md`.
