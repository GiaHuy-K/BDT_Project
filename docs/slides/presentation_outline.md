# BDT301 Group Project Presentation: Slide Deck & Demo Script
## End-to-End Big Data Analytics Pipeline: Grupo Bimbo
**Thời lượng trình bày:** 10–15 phút (+ Demo & Q&A)

---

### Slide 1: Giới thiệu Dự án & Thành viên
* **Tiêu đề:** Xây dựng Pipeline Big Data Phân tích Nhu cầu & Hàng trả lại trong Chuỗi phân phối Grupo Bimbo.
* **Môn học:** BDT301 – Big Data Technologies.
* **Thành viên & Phân công:**
  1. *Thành viên 1:* Ingestion & Hạ tầng Docker (MinIO, PostgreSQL, Spark Master/Workers).
  2. *Thành viên 2:* Spark Processing (Tầng Bronze & Silver, Data Cleaning, Invariant Check).
  3. *Thành viên 3:* Spark Processing (Tầng Gold, Thuật toán Anomaly Detection, Phân khúc RFM).
  4. *Thành viên 4:* Phân tích SQL, Serving Layer & Xây dựng Metabase Dashboard.
  5. *Thành viên 5:* Tích hợp Pipeline (`run.sh`), Tài liệu kỹ thuật & Đánh giá Trade-offs.

---

### Slide 2: Bài toán Doanh nghiệp & 4 Câu hỏi Nghiệp vụ
* **Vấn đề:** Dữ liệu Grupo Bimbo có 930,500 mã điểm bán, 552 agency, 74.2 triệu dòng giao dịch trong 7 tuần. Sản phẩm bánh mì có hạn dùng ngắn, hàng tồn quá hạn phải thu hồi (`Dev_proxima`).
* **Mục tiêu:** Tối ưu hóa chuỗi cung ứng, giảm chi phí tiêu hủy hàng trả lại.
* **4 Câu hỏi Nghiệp vụ:**
  * **Q1:** Xu hướng nhu cầu và doanh thu theo tuần, bang và kênh bán lẻ?
  * **Q2:** Những sản phẩm, đại lý, tuyến giao hàng nào có tỷ lệ trả hàng cao bất thường?
  * **Q3:** Top 10 sản phẩm bán chạy nhất và thị phần thương hiệu theo từng bang?
  * **Q4:** Điểm bán lẻ được phân khúc như thế nào theo hành vi mua hàng và rủi ro hoàn hàng?

---

### Slide 3: Kiến trúc Tổng thể (Medallion Architecture)
* **Data Flow:**
  $$\text{Kaggle Zip} \xrightarrow{\text{Ingestion}} \text{MinIO Raw} \xrightarrow{\text{Spark Bronze}} \text{Silver} \xrightarrow{\text{Spark Gold}} \text{PostgreSQL} \xrightarrow{\text{Metabase Dashboard}}$$
* **Lý do lựa chọn công nghệ:**
  * *MinIO:* Tương thích chuẩn S3, tách biệt hoàn toàn giữa Storage và Compute.
  * *Apache Parquet:* Định dạng cột nén cao, hỗ trợ Column Pruning & Predicate Pushdown.
  * *Apache Spark 3.5:* Động cơ tính toán phân tán in-memory DAG cho 74.2M dòng.
  * *PostgreSQL 16 + Metabase:* Bảng Gold đã tổng hợp nhỏ, SQL chuẩn, Metabase kết nối sẵn.

---

### Slide 4: Ingestion & Tầng Bronze
* **Bộ dữ liệu:** 74.2 triệu dòng `train.csv` (~3.2 GB), tổng cộng ~3.5 GB raw.
* **Ingestion tự động:**
  * Script `ingest.py` tải trực tiếp vào MinIO bucket `raw`.
  * Tính SHA-256 khi stream: file không đổi thì bỏ qua (idempotent); sinh manifest ghi số byte, số dòng, checksum.
* **Bronze Layer:**
  * Ép kiểu tĩnh tường minh (`StructType`) loại bỏ rủi ro sai kiểu dữ liệu.
  * Phân vùng theo tuần giao hàng (`partitionBy("Semana")`) giúp tối ưu truy vấn thời gian.

---

### Slide 5: Tầng Silver — Làm sạch & Bảo toàn Dữ liệu (Invariant Check)
* **Khử trùng lặp:** 935,362 dòng `cliente_tabla` → 930,500 `Cliente_ID` duy nhất (bỏ 4,862 dòng trùng), tên được chuẩn hóa in hoa, bỏ dấu.
* **Regex trích xuất đặc trưng sản phẩm:** Tách riêng số cái (`pieces`), trọng lượng (`weight_g`), và thương hiệu (`brand`).
* **Broadcast Join:** Broadcast 3 bảng dimension (lớn nhất là `dim_cliente` 10.4 MB Parquet) sang các executor, nên join với 74.2 triệu dòng fact không cần shuffle bảng fact (tránh skew ở kênh 1).
* **Nguyên tắc Bảo toàn Dữ liệu (Invariant Check):**
  $$\text{fact\_sales (74,180,464 dòng)} + \text{rejected (0 dòng)} = \text{bronze\_train (74,180,464 dòng)}$$
  $\to$ Đảm bảo không thất thoát bất kỳ dòng dữ liệu nào!

---

### Slide 6: Tầng Gold — Xử lý Tổng hợp Phục vụ Business
* **Q1 (Window function):** Tính toán biến động tuần qua tuần (`wow_demand_change_pct`, `wow_sales_change_pct`).
* **Q2 (Anomaly Detection với 2-Sigma):**
  * Thiết lập ngưỡng kiểm soát thống kê: $\text{Threshold} = \mu_{\text{return\_rate}} + 2\sigma_{\text{return\_rate}}$.
  * Tính trên tổ hợp sản phẩm × agency × tuyến bán ≥ 20 đơn vị; ngưỡng thực tế = 31.52%. Gắn cờ `is_anomaly = True` cho 20,208 tổ hợp vượt ngưỡng.
* **Q3 (Ranking đa chiều):** Dùng `dense_rank()` phân hạng sản phẩm và tính tỷ trọng đóng góp (`revenue_share_pct`) trên tổng doanh thu từng bang.
* **Q4 (Phân khúc rule-based):** Phân loại 880,604 khách hàng có giao dịch trong `train` thành 6 nhóm: *Unknown/Unidentified*, *VIP/High-Value*, *High Return Risk*, *Regular/Loyal*, *Occasional/Churn Risk*, *Standard*.

---

### Slide 7: Kết quả Phân tích & Giá trị Nghiệp vụ
* **Bảng điều khiển Tổng quan (Metabase Dashboard):** <!-- TODO: chèn ảnh dashboard dựng trên dữ liệu thật -->
* **Q1:** Nhu cầu ổn định 73.9–79.6 triệu đơn vị/tuần, nhưng hàng trả tăng 27.6% từ tuần 3 đến tuần 9 (tỷ lệ trả/nhu cầu 1.66% → 2.19%). Kênh 1 có 91% số dòng nhưng chỉ 61.8% doanh thu; kênh 2 có 9.6% nhu cầu nhưng 18.5% doanh thu.
* **Q2:** 20,208 tổ hợp dị biệt (2.2%) chỉ tạo 0.5% doanh thu nhưng chiếm 16.6% giá trị hàng trả.
* **Q3:** Pan Blanco 640g nằm trong top 3 ở 25/32 bang; brand BIM chiếm 59.8% doanh thu thuần, MLA 19.1%, TR 8.7%.
* **Q4:** Nhóm VIP chiếm 19.4% số điểm bán nhưng 71.3% doanh thu thuần; 1% điểm bán lớn nhất đã chiếm 33.2%. Nhóm High Return Risk có tỷ lệ trả 45.8% theo giá trị, gấp khoảng 25 lần mức chung.

---

### Slide 8: Độ tin cậy & Đánh đổi Kiến trúc (Trade-offs)
* **Trade-off 1 (Batch vs. Streaming):** Dữ liệu đến theo tuần $	o$ batch là đủ; toàn bộ pipeline chạy hết 7 phút 16 giây, không cần duy trì cụm streaming 24/7. Đổi lại, kết quả trễ một lần chạy.
* **Trade-off 2 (Normalized vs. Denormalized):** Silver giữ star schema; Gold làm phẳng còn 1.96 triệu dòng, nên query Q4 trên PostgreSQL chỉ mất ~46 ms. Đổi lại, dữ liệu bị lặp và phải chạy lại Gold khi dimension đổi.
* **Phòng ngừa lỗi OOM:** Tránh lạm dụng `.cache()`, dùng Broadcast Join và phân vùng thông minh.

---

### Slide 9: Kế hoạch Demo Thực tế (Live Demo Script)
1. **Kiểm tra trạng thái hạ tầng:** `docker compose ps` (MinIO, Spark Master/Workers, History Server, Postgres, Metabase).
2. **MinIO Console (cổng 9001):** Cho xem bucket `raw` (CSV gốc) và bucket `processed` (Parquet theo phân tầng bronze/silver/gold).
3. **Spark History Server (cổng 18080):** Mở xem DAG thực thi các jobs `bimbo-bronze`, `bimbo-silver`, `bimbo-gold` với Application ID tương ứng trong file logs.
4. **PostgreSQL / Metabase (cổng 3000):** Chạy câu lệnh SQL trả lời 4 câu hỏi nghiệp vụ và hiển thị biểu đồ phân tích trực quan.

---

### Slide 10: Bộ câu hỏi Q&A thường gặp của Giảng viên
* *Hỏi:* Tại sao không dùng MongoDB hay Cassandra mà lại dùng PostgreSQL ở serving layer?
  * *Trả lời:* Dữ liệu tầng Gold đã được Spark tổng hợp thành 4 bảng từ 1,393 đến 1.06 triệu dòng. PostgreSQL với chuẩn quan hệ ANSI SQL, có thể thêm index khi cần (hiện chưa cần) và kết nối native với Metabase là giải pháp nhẹ nhàng, tin cậy và tối ưu nhất cho báo cáo quản trị.
* *Hỏi:* Làm thế nào để chứng minh Spark đã chạy trên toàn bộ tập dữ liệu chứ không phải chạy trên mẫu thử?
  * *Trả lời:* Trong `logs/evidence/run_20261029_205823.log` (4 application ID: bronze `app-20261029140008-0000`, silver `-0001`, gold `-0002`, export `-0003`) và `silver_quality_app-20261029140124-0001.json`, bronze và fact đều đếm đúng 74,180,464 dòng, khớp số dòng `train.csv`. Event log tương ứng xem được trên Spark History Server.
