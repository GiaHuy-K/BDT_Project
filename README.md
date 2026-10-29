# BDT_Project – Grupo Bimbo Big Data Pipeline

BDT301 Group Project: pipeline batch end-to-end phân tích nhu cầu và hàng trả lại trong chuỗi phân phối bánh Grupo Bimbo (74.2 triệu dòng, ~3.5 GB).

```
Kaggle ─► ingest.py ─► MinIO raw/ ─► Spark bronze ─► silver ─► gold ─► PostgreSQL ─► Metabase
```

## Câu hỏi business

| # | Câu hỏi | Bảng gold |
| --- | --- | --- |
| Q1 | Nhu cầu thay đổi thế nào theo tuần, bang, kênh? | `weekly_demand_by_state_channel` |
| Q2 | Sản phẩm/agency/tuyến nào trả hàng cao bất thường? | `return_rate_by_product_agency_route` |
| Q3 | Sản phẩm/brand nào dẫn đầu doanh thu từng bang? | `product_ranking_by_state` |
| Q4 | Điểm bán chia thành những nhóm nào? | `client_segments` |

## Cấu trúc repo

```
BDT_Project/
├── README.md
├── docker-compose.yml        # MinIO, Spark (master, 2 workers, history), PostgreSQL, Metabase
├── run.sh                    # chạy toàn bộ pipeline theo thứ tự, ghi log vào logs/
├── .env.example              # cấu hình mẫu -> copy thành .env
├── requirements.txt          # thư viện Python cho ingestion (chạy trên máy host)
├── config/
│   └── spark-defaults.conf   # tài nguyên Spark, gói S3A/JDBC, event log
├── data/                     # zip Kaggle (không commit, ingest.py tự tải)
├── ingestion/
│   └── ingest.py             # zip -> MinIO raw/, checksum, manifest
├── spark_jobs/
│   ├── common/               # config đường dẫn, SparkSession, schema
│   ├── bronze.py             # CSV -> Parquet có kiểu, partition theo Semana
│   ├── silver.py             # làm sạch, dedup, tách tên sản phẩm, join dim
│   ├── gold.py               # 4 bảng tổng hợp cho Q1–Q4
│   └── export_postgres.py    # gold -> PostgreSQL schema gold
├── sql/
│   ├── init/                 # tạo DB metabase + schema gold
│   └── analysis/             # query trả lời Q1–Q4
├── dashboard/                # hướng dẫn dựng dashboard Metabase
├── notebooks/                # EDA, thử nghiệm
├── logs/
│   ├── spark-events/         # Spark event log (xem ở History Server)
│   └── evidence/             # log chạy cuối cùng được commit làm bằng chứng
└── docs/
    ├── architecture.md
    ├── dataset.md
    ├── contribution.md
    ├── report/               # báo cáo 5–8 trang
    ├── slides/               # slide thuyết trình
    └── requirements/         # đề bài + rubric BDT301
```

Phân vùng lưu trữ trong MinIO:

| Bucket | Đường dẫn | Nội dung |
| --- | --- | --- |
| `raw` | `grupo_bimbo/<table>/<table>.csv`, `grupo_bimbo/_manifests/` | CSV gốc, không bao giờ sửa |
| `processed` | `bronze/`, `silver/`, `gold/` | Parquet do Spark ghi |

## Yêu cầu

- Docker Desktop (khuyến nghị cấp ≥ 8 GB RAM)
- Python 3.10+ và Git Bash (Windows) để chạy `run.sh`
- Tài khoản Kaggle đã bấm "Join competition" và file `~/.kaggle/kaggle.json` (chỉ cần nếu `data/` chưa có zip)

## Cách chạy

```bash
pip install -r requirements.txt
```

```bash
./run.sh
```

`run.sh` chạy lần lượt: `infra` → `ingest` → `bronze` → `silver` → `gold` → `export`. Muốn chạy lại từ một bước thì truyền tên bước đó, ví dụ:

```bash
./run.sh silver
```

## Kết quả mong đợi

| Service | URL |
| --- | --- |
| MinIO console | http://localhost:9001 |
| Spark master UI | http://localhost:8080 |
| Spark History Server | http://localhost:18080 |
| Metabase | http://localhost:3000 |

- `logs/run_<timestamp>.log` chứa log toàn bộ lần chạy, kể cả `applicationId` của từng Spark job.
- `logs/ingest_<timestamp>.json` là manifest ingestion (số dòng, sha256).
- Bảng `gold.*` trong PostgreSQL; query mẫu nằm ở `sql/analysis/`.

## Trạng thái

- [x] Hạ tầng Docker Compose, ingestion, bronze, export
- [x] Silver: làm sạch, dedup, tách tên sản phẩm, bảng `rejected`, kiểm tra bất biến
- [x] Gold: 4 bảng Q1–Q4 (lần chạy bằng chứng: `logs/evidence/run_20261029_205823.log`)
- [x] Báo cáo (`docs/report/`), dàn ý slide (`docs/slides/`)
- [ ] Ảnh dashboard Metabase dựng trên dữ liệu thật (`docs/report/dashboard.png`)
- [ ] Điền tên thành viên vào `docs/contribution.md`
