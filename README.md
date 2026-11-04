# BDT_Project – Grupo Bimbo Big Data Pipeline

BDT301 Group Project: pipeline batch end-to-end phân tích nhu cầu và hàng trả lại trong chuỗi phân phối bánh Grupo Bimbo (74.2 triệu dòng, ~3.5 GB).

```
Kaggle ─► ingest.py ─► MinIO raw/ ─► Spark bronze ─► silver ─► gold ─► forecast ─► PostgreSQL ─► Metabase
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
├── notebooks/                # biểu đồ và số liệu EDA, dự báo
├── docker/spark/             # image Spark có thêm numpy cho MLlib
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

- Docker Desktop (khuyến nghị cấp ≥ 8 GB RAM) và **≥ 40 GB trống trên ổ chứa dữ liệu Docker** (raw 3.3 GB + các tầng Parquet trong MinIO, image, vùng tạm của Spark). Trên Windows dữ liệu Docker mặc định ở ổ C (`%LOCALAPPDATA%\Docker\wsl`); nếu C chật, đổi trong Docker Desktop → Settings → Resources → Advanced → *Disk image location*.
- Python 3.10+ và Git Bash (Windows) để chạy `run.sh` (`run.sh` tự đặt `MSYS_NO_PATHCONV=1` để Git Bash không đổi đường dẫn `/opt/spark/...` trong container)
- Tài khoản Kaggle đã bấm "Join competition" và file `~/.kaggle/kaggle.json` (chỉ cần nếu `data/` chưa có zip)

## Cách chạy

```bash
pip install -r requirements.txt
```

```bash
./run.sh
```

`run.sh` chạy lần lượt: `infra` → `ingest` → `bronze` → `silver` → `gold` → `forecast` → `export`. Lần chạy đầu Docker tự build image Spark có thêm `numpy` (`docker/spark/Dockerfile`), vì MLlib cần thư viện này. Muốn chạy lại từ một bước thì truyền tên bước đó, ví dụ:

```bash
./run.sh silver
```

Bước `forecast` lâu nhất. Khi chỉ cần chạy lại gold và export, có thể bỏ qua bước đó và giữ dự báo gần nhất:

```bash
SKIP_FORECAST=1 ./run.sh gold
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
- Biểu đồ của báo cáo vẽ lại từ PostgreSQL bằng `python docs/report/make_figures.py`.

## Dự báo và ngưỡng outlier theo kênh

Hai phần này được thêm vào pipeline sau khi EDA cho thấy lý do cần có chúng:

- **`is_outlier_channel`** (silver, bảng `gold.outliers_by_channel`): mỗi kênh dùng phân vị 99.9% của chính nó. Cột `is_outlier` cũ giữ nguyên.
- **`spark_jobs/forecast.py`** (bảng `gold.forecast_metrics`, `gold.forecast_demand_by_state_channel`): GBT của Spark MLlib, học trên tuần 6–8, kiểm tra trên tuần 9 với khách chưa dùng để học. RMSLE 0.475 so với 0.529 của baseline, tức giảm 10.3%. Khi cộng lên bang × kênh thì baseline chính xác hơn (sai 5.3% so với 7.3%), nên bảng kết quả lưu cả hai. `test.csv` chỉ có khoảng 1/3 số dòng của một tuần, nên tổng dự báo không phải nhu cầu toàn quốc.

Query: `sql/analysis/q5_forecast.sql`, `sql/analysis/outliers_by_channel.sql`. Biểu đồ: `python notebooks/forecast_figures.py`.

## EDA (tùy chọn, chạy sau `./run.sh`)

Job chỉ đọc dữ liệu và không nằm trong `run.sh`, nên không làm thay đổi pipeline. Kết quả, giải thích từng biểu đồ: [`docs/eda/EDA_report.md`](docs/eda/EDA_report.md).

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/pipeline/spark_jobs/eda.py
```

```bash
python notebooks/eda_report.py
```

Nếu máy đã cài sẵn PostgreSQL chiếm cổng 5432, đổi `POSTGRES_PORT` trong `.env` (ví dụ 5433) rồi `docker compose up -d postgres`. Spark, Metabase và script vẽ biểu đồ kết nối trong mạng Docker nên không bị ảnh hưởng.

## Trạng thái

- [x] Hạ tầng Docker Compose, ingestion, bronze, export
- [x] Silver: làm sạch, dedup, tách tên sản phẩm, bảng `rejected`, kiểm tra bất biến
- [x] Gold: 4 bảng Q1–Q4, outlier theo kênh, dự báo (lần chạy bằng chứng: `logs/evidence/run_20261104_112206.log`)
- [x] Báo cáo (`docs/report/`), dàn ý slide (`docs/slides/`)
- [x] Dashboard Metabase Q1–Q4 (SQL: `dashboard/cards.sql`, ảnh: `docs/report/dashboard.png`)
- [x] Bảng phân công 3 thành viên (`docs/contribution.md`)
