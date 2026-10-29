# Contribution record

Nhóm 3 · BDT301 Group Project · 3 thành viên. Phần việc của mỗi người khớp với các commit của họ trong lịch sử Git của repo.

| Thành viên | Checkpoint | Đã hoàn thành | File / thư mục chính | Commit |
| --- | --- | --- | --- | --- |
| **Khổng Trần Gia Huy** | Chọn dataset + Checkpoint 1 | Tìm và đánh giá dataset Grupo Bimbo trên Kaggle; proposal Checkpoint 1 (bài toán, 4 câu hỏi business, kiến trúc ban đầu); dựng cấu trúc repo, Docker Compose, script ingestion, job bronze, khung job silver/gold, README; tích hợp bản sửa tài liệu, biểu đồ và ghi chú bằng chứng cuối cùng | `ingestion/`, `docker-compose.yml`, `config/`, `spark_jobs/bronze.py`, `spark_jobs/common/`, `README.md` | `674dd06` (18/10), `050c0b2`, `c86f248` (29/10) |
| **Bùi Anh Duy** | Checkpoint 2 | Tầng silver: làm sạch và dedup dimension, tách tên sản phẩm bằng regex, quy tắc reject kèm lý do, cờ chất lượng, kiểm tra bất biến fact + rejected = bronze, báo cáo chất lượng; sửa Docker Compose (MinIO bản Bitnami, mount logs) và `run.sh` (healthcheck, đường dẫn Git Bash); chạy pipeline trên toàn bộ 74.2 triệu dòng và lưu bằng chứng | `spark_jobs/silver.py`, `run.sh`, `logs/evidence/` | `b7ef14a` (19/10) |
| **Nguyễn Võ Tuấn Kiệt** | Checkpoint 3 | Tầng gold: 4 bảng tổng hợp Q1–Q4; 4 query phân tích SQL; hướng dẫn dashboard Metabase; tài liệu kiến trúc; báo cáo và dàn ý slide; cho ingestion đọc được thư mục đã giải nén | `spark_jobs/gold.py`, `sql/analysis/`, `dashboard/`, `docs/architecture.md`, `docs/report/`, `docs/slides/` | `2c40970` (19/10) |

## Phân công trình bày (đề xuất)

Mỗi người trình bày phần mình làm, để trả lời được câu hỏi về code của chính mình:

| Thành viên | Slide | Nội dung |
| --- | --- | --- |
| Khổng Trần Gia Huy | 1–5 | Bài toán, 4 câu hỏi, dataset, kiến trúc, ingestion và bronze |
| Bùi Anh Duy | 6, 12 | Silver, kiểm tra bất biến, độ tin cậy và trade-off |
| Nguyễn Võ Tuấn Kiệt | 7, slide dashboard, 8–11, 13 | Gold, dashboard, kết quả Q1–Q4, demo |
| Cả nhóm | Kết luận | Hạn chế, hướng phát triển, Q&A |

## Công cụ hỗ trợ

Nhóm có dùng trợ lý AI (Claude) để hỗ trợ viết code, rà soát và soạn tài liệu. Mỗi thành viên chịu trách nhiệm hiểu, giải thích và sửa được phần mình phụ trách, theo yêu cầu của đề bài.
