# Processing evidence

**Bằng chứng chính là lần chạy ngày 29/10/2026.** Đây là lần duy nhất cả 4 bảng Gold (Q1–Q4) được sinh từ toàn bộ dữ liệu, và mọi số liệu trong report, slide đều lấy từ lần chạy này.

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
