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
* **Vấn đề:** Grupo Bimbo có hơn 1 triệu điểm bán lẻ tại Mexico, 74.2 triệu giao dịch/7 tuần. Sản phẩm bánh mì có hạn dùng ngắn, hàng tồn quá hạn phải thu hồi (`Dev_proxima`).
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
  * *PostgreSQL 16 + Metabase:* Tầng serving truy vấn dưới 100ms và dashboard trực quan.

---

### Slide 4: Ingestion & Tầng Bronze
* **Bộ dữ liệu:** 74.2 triệu dòng `train.csv` (~3.2 GB), tổng cộng ~3.5 GB raw.
* **Ingestion tự động:**
  * Script `ingest.py` tải trực tiếp vào MinIO bucket `raw`.
  * Tính mã băm MD5 để đảm bảo tính bất biến (idempotent), sinh file manifest ghi nhận vết.
* **Bronze Layer:**
  * Ép kiểu tĩnh tường minh (`StructType`) loại bỏ rủi ro sai kiểu dữ liệu.
  * Phân vùng theo tuần giao hàng (`partitionBy("Semana")`) giúp tối ưu truy vấn thời gian.

---

### Slide 5: Tầng Silver — Làm sạch & Bảo toàn Dữ liệu (Invariant Check)
* **Khử trùng lặp:** 4,862 mã khách hàng trùng lặp trong dimension được dedup và chuẩn hóa text.
* **Regex trích xuất đặc trưng sản phẩm:** Tách riêng số cái (`pieces`), trọng lượng (`weight_g`), và thương hiệu (`brand`).
* **Broadcast Join:** Broadcast 3 bảng dimension (< 50MB) sang các worker, loại bỏ hoàn toàn hiện tượng nghẽn mạng do Data Skew ở kênh bán lẻ 1.
* **Nguyên tắc Bảo toàn Dữ liệu (Invariant Check):**
  $$\text{fact\_sales (74,180,464 dòng)} + \text{rejected (0 dòng)} = \text{bronze\_train (74,180,464 dòng)}$$
  $\to$ Đảm bảo không thất thoát bất kỳ dòng dữ liệu nào!

---

### Slide 6: Tầng Gold — Xử lý Tổng hợp Phục vụ Business
* **Q1 (Window function):** Tính toán biến động tuần qua tuần (`wow_demand_change_pct`, `wow_sales_change_pct`).
* **Q2 (Anomaly Detection với 2-Sigma):**
  * Thiết lập ngưỡng kiểm soát thống kê: $\text{Threshold} = \mu_{\text{return\_rate}} + 2\sigma_{\text{return\_rate}}$.
  * Gắn cờ `is_anomaly = True` cho các tuyến đường vượt ngưỡng cảnh báo.
* **Q3 (Ranking đa chiều):** Dùng `dense_rank()` phân hạng sản phẩm và tính tỷ trọng đóng góp (`revenue_share_pct`) trên tổng doanh thu từng bang.
* **Q4 (Phân khúc RFM):** Phân loại tự động 930,500 khách hàng thành 5 nhóm: *VIP/High-Value*, *Regular/Loyal*, *High Return Risk*, *Occasional/Churn Risk*, *Standard*.

---

### Slide 7: Kết quả Phân tích & Giá trị Nghiệp vụ
* **Bảng điều khiển Tổng quan (Metabase Dashboard):**
![alt text](image.png)
* **Kênh 1 (Tạp hóa nhỏ):** Chiếm > 90% sản lượng nhưng biên lợi nhuận thấp hơn các kênh bán sỉ.
* **Nguy cơ trả hàng:** Các dòng bánh mì tươi hạn sử dụng siêu ngắn (< 5 ngày) chiếm 80% giá trị hàng trả tại các đại lý xa trung tâm.
* **Quy luật Pareto ở khách hàng:** Nhóm VIP chỉ chiếm ~8% số lượng điểm bán nhưng đóng góp tới 62% tổng doanh thu toàn mạng lưới.

---

### Slide 8: Độ tin cậy & Đánh đổi Kiến trúc (Trade-offs)
* **Trade-off 1 (Batch vs. Streaming):** Chu kỳ giao nhận bánh theo tuần $\to$ Batch Processing với Spark định kỳ mang lại thông lượng tối đa với chi phí hạ tầng thấp nhất.
* **Trade-off 2 (Normalized vs. Denormalized):** Tầng Silver chuẩn hóa tối ưu dung lượng đĩa $\to$ Tầng Gold làm phẳng tổng hợp để serving cực nhanh trên Metabase.
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
  * *Trả lời:* Dữ liệu tầng Gold đã được Spark nén thành các bảng tổng hợp có kích thước nhỏ (hàng ngàn đến dưới 1 triệu dòng). PostgreSQL với chuẩn quan hệ ANSI SQL, khả năng đánh index và kết nối native với Metabase là giải pháp nhẹ nhàng, tin cậy và tối ưu nhất cho báo cáo quản trị.
* *Hỏi:* Làm thế nào để chứng minh Spark đã chạy trên toàn bộ tập dữ liệu chứ không phải chạy trên mẫu thử?
  * *Trả lời:* Trong file log `run_20261019_162611.log` và file JSON `silver_quality_app-20261019092920-0005.json`, số lượng dòng được đếm chính xác là 74,180,464 dòng (khớp 100% với số dòng `train.csv` của Kaggle) và đã được lưu trữ trong Spark Event Log.
