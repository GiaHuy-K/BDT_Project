# Data Architecture & Technology Justification

## 1. End-to-End Data Pipeline Overview

Hệ thống được thiết kế theo mô hình **Medallion Architecture (Bronze - Silver - Gold)** kết hợp tầng phục vụ phân tích (Serving Layer) và trực quan hóa (BI Dashboard).

```
                      ┌───────────────────────────────────────────┐
                      │           Kaggle Competition Data         │
                      │       (74.2M rows train.csv, ~3.5 GB)     │
                      └─────────────────────┬─────────────────────┘
                                            │ Python Ingestion (MD5 & Manifest)
                                            ▼
                      ┌───────────────────────────────────────────┐
                      │          MinIO Object Storage             │
                      │               s3a://raw                   │
                      │         (Original Immutable CSV)          │
                      └─────────────────────┬─────────────────────┘
                                            │ PySpark: bronze.py (Schema Enforcement)
                                            ▼
                      ┌───────────────────────────────────────────┐
                      │          MinIO Object Storage             │
                      │             s3a://processed               │
                      │  Bronze: Typed Parquet (partition: Semana)│
                      └─────────────────────┬─────────────────────┘
                                            │ PySpark: silver.py (Clean, Dedup, Dims, Invariant)
                                            ▼
                      ┌───────────────────────────────────────────┐
                      │  Silver: Fact Sales + Dimensions + Rejects│
                      │       (Invariant: Fact + Rejects = Train) │
                      └─────────────────────┬─────────────────────┘
                                            │ PySpark: gold.py (Q1-Q4 Aggregations & ML/Stats)
                                            ▼
                      ┌───────────────────────────────────────────┐
                      │  Gold: Denormalized Aggregates (Parquet)  │
                      │   - weekly_demand_by_state_channel        │
                      │   - return_rate_by_product_agency_route   │
                      │   - product_ranking_by_state              │
                      │   - client_segments                       │
                      └─────────────────────┬─────────────────────┘
                                            │ PySpark: export_postgres.py (JDBC)
                                            ▼
                      ┌───────────────────────────────────────────┐
                      │          PostgreSQL 16 (Serving)          │
                      │           Schema: gold (Indexed)          │
                      └─────────────────────┬─────────────────────┘
                                            │ SQL Analysis Queries
                                            ▼
                      ┌───────────────────────────────────────────┐
                      │          Metabase BI Dashboard            │
                      │      (Visual Charts, KPIs & Insights)     │
                      └───────────────────────────────────────────┘
```

---

## 2. Technology Selection & Rationale

| Tầng (Layer) | Công nghệ lựa chọn | Giải pháp thay thế cân nhắc | Lý do lựa chọn (Design Rationale) |
| :--- | :--- | :--- | :--- |
| **Ingestion** | Python 3 + MinIO SDK | Bash curl / Manual copy | Tự động hóa tải zip, kiểm tra MD5 checksum chống trùng lặp dữ liệu, sinh manifest metadata cho audit trail. |
| **Raw & Processed Storage** | MinIO (S3-compatible Object Storage) | HDFS, Local Filesystem | Nhẹ, dễ triển khai qua Docker, tương thích 100% chuẩn S3 API của cloud hiện đại (AWS S3, GCS). Tách biệt hoàn toàn compute và storage. |
| **Storage Format** | Apache Parquet (Snappy) | CSV, JSON | Định dạng cột (columnar format) nén tốt gấp 3-4 lần CSV, hỗ trợ predicate pushdown và column pruning, giảm I/O cho Spark. |
| **Processing Engine** | Apache Spark 3.5 (Standalone) | Pandas, MapReduce, Dask | Dữ liệu > 74 triệu dòng vượt giới hạn RAM của Pandas (gây OOM). Spark xử lý song song phân tán (in-memory DAG execution) với Catalyst Optimizer và Tungsten Engine. |
| **Serving Layer** | PostgreSQL 16 (Schema `gold`) | MySQL, MongoDB, Hive | Hỗ trợ chuẩn ANSI SQL mạnh mẽ, schema relational nhất quán, tốc độ truy vấn phân tích tổng hợp cực nhanh khi dữ liệu đã được Spark nén qua Gold. |
| **BI & Analytics** | Metabase | Apache Superset, Tableau | Giao diện hiện đại, trực quan, hỗ trợ viết SQL trực tiếp, kết nối native với PostgreSQL, triển khai nhanh gọn qua Docker. |
| **Orchestration** | Docker Compose + `run.sh` | Apache Airflow | Đảm bảo tính tái lập (reproducible) trên mọi hệ điều hành (Windows, Linux, macOS) chỉ với 1 lệnh mà không tạo chi phí quản trị cụm quá lớn. |

---

## 3. Reliability, Quality Control & Invariants

Hệ thống thiết lập các chốt kiểm soát chất lượng dữ liệu ở từng chặng:

1. **Ingestion Idempotency & Checksum:**
   - Script `ingest.py` tính MD5 hash của từng file CSV. Nếu file đã tồn tại và không thay đổi hash trên MinIO, ingestion tự động bỏ qua để tiết kiệm tài nguyên.
   - Sinh file manifest `ingest_<timestamp>.json` lưu vết kích thước, ngày nạp, trạng thái.

2. **Schema Enforcement & Partitioning ở Bronze:**
   - Khi đọc CSV gốc, áp dụng schema tường minh (`StructType`) thay vì suy diễn kiểu dữ liệu (`inferSchema = True`) nhằm tránh lỗi sai lệch kiểu dữ liệu hoặc đọc nhầm kiểu số thành chuỗi.
   - Phân vùng (`partitionBy("Semana")`) cho fact tables, tối ưu hóa thời gian quét dữ liệu theo tuần giao hàng.

3. **Bảo toàn dữ liệu tuyệt đối ở Silver (Invariant Check):**
   - Đảm bảo quy tắc bảo toàn:
     $$\text{Row Count}(\text{fact\_sales}) + \text{Row Count}(\text{rejected}) = \text{Row Count}(\text{bronze\_train})$$
   - Mọi dòng lỗi (khóa ngoại null, số âm, doanh thu bất hợp lý) được tách sang bảng `rejected` kèm cột `reason` cụ thể, không bao giờ âm thầm drop dữ liệu.
   - Xuất file chất lượng `silver_quality_<app_id>.json` ghi nhận chính xác tỷ lệ outlier, zero demand, unknown clients.

4. **Khử trùng Dimension (Deduplication):**
   - `cliente_tabla.csv` chứa 4,862 ID khách hàng bị lặp lại trong dữ liệu gốc. Job Silver thực hiện dedup và chuẩn hóa chữ in hoa không dấu trước khi broadcast join vào bảng Fact.

---

## 4. Architecture Trade-offs Analysis

Đề tài phân tích và chấp nhận các đánh đổi kiến trúc sau:

### Trade-off 1: Batch Processing vs. Real-time Streaming
- **Lựa chọn:** Batch Processing (chu kỳ theo tuần / ngày).
- **Phân tích:** Ngành hàng bánh tươi (Grupo Bimbo) có chu kỳ lập kế hoạch sản xuất và tuyến giao nhận cố định theo tuần (`Semana 3 -> 9`). Dữ liệu hoàn hàng (`Dev_proxima`) chỉ được ghi nhận vào chu kỳ giao hàng tiếp theo.
- **Đánh đổi:** Chấp nhận độ trễ (latency vài chục phút cho batch job) để đổi lấy thông lượng (throughput) cực lớn khi quét 74 triệu dòng, tiết kiệm chi phí hạ tầng so với việc duy trì cluster Kafka + Spark Streaming 24/7.

### Trade-off 2: Storage Normalization vs. Gold Denormalization
- **Lựa chọn:** Mô hình chuẩn hóa (Star Schema) ở tầng Silver, Denormalized Summary Table ở tầng Gold.
- **Phân tích:** Tầng Silver lưu trữ `fact_sales` và 3 bảng chiều (`dim_cliente`, `dim_producto`, `dim_agencia`) để tiết kiệm dung lượng lưu trữ Parquet và dễ bảo trì. Tầng Gold làm phẳng (denormalize) và tính toán sẵn các chỉ số (agg) cho từng câu hỏi kinh doanh.
- **Đánh đổi:** Tốn thêm một lượng nhỏ dung lượng đĩa ở Gold để đổi lấy tốc độ truy vấn mili-giây trên PostgreSQL/Metabase khi người dùng xem dashboard mà không phải join hàng chục triệu dòng tại thời điểm truy vấn.

### Trade-off 3: Data Skew Handling vs. Broadcast Join
- **Phân tích:** Kênh phân phối 1 (`Canal_ID = 1`) chiếm tới hơn 90% số lượng giao dịch. Nếu shuffle join bảng lớn với bảng lớn sẽ gây nghẽn tại 1 vài partition (data skew).
- **Giải pháp:** Vì các bảng chiều (`dim_cliente`, `dim_producto`, `dim_agencia`) sau khi làm sạch có kích thước nhỏ (< 50MB), Spark áp dụng `F.broadcast(dim)` chuyển bảng chiều đến từng executor, loại bỏ hoàn toàn giai đoạn shuffle data skew khi tạo `fact_sales`.
