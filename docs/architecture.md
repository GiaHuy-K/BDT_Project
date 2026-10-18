# Architecture

```
Kaggle zip ──► ingestion/ingest.py ──► MinIO raw/ (CSV gốc)
                                            │
                                            ▼
                       Spark: bronze ─► silver ─► gold   (MinIO processed/, Parquet)
                                                    │
                                                    ▼
                                    PostgreSQL schema gold ──► Metabase
```

| Khối | Công nghệ | Thư mục / service |
| --- | --- | --- |
| Ingestion | Python + Kaggle API + MinIO client | `ingestion/` |
| Raw storage | MinIO bucket `raw` | service `minio` |
| Processing | Apache Spark 3.5 standalone (1 master, 2 worker) | `spark_jobs/`, `config/spark-defaults.conf` |
| Processed storage | MinIO bucket `processed` (bronze/silver/gold Parquet) | service `minio` |
| Serving | PostgreSQL 16, schema `gold` | `sql/init/`, service `postgres` |
| Dashboard | Metabase | `dashboard/`, `sql/analysis/` |
| Orchestration | Docker Compose + `run.sh` | repo root |

Lý do chọn công nghệ và trade-off: xem proposal Checkpoint 1.
TODO: thêm ảnh sơ đồ kiến trúc (`docs/architecture.png`) cho báo cáo.
