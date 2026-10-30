# Hướng dẫn Thiết lập Dashboard Metabase (Grupo Bimbo Analytics)

Bảng điều khiển (Dashboard) trên **Metabase** đóng vai trò là tầng phân tích & trực quan hóa (Presentation Layer) cho người dùng nghiệp vụ và ban quản lý chuỗi cung ứng.

---

## 1. Kết nối Metabase với PostgreSQL

1. Chạy `./run.sh` cho đến bước `export`. Các bảng `gold.*` chỉ có trong PostgreSQL sau khi pipeline chạy xong; repo không còn seed dữ liệu mẫu, nên dashboard luôn hiển thị kết quả thật của Spark.
2. Mở trình duyệt web tại địa chỉ: [http://localhost:3000](http://localhost:3000).
3. Hoàn thành bước tạo tài khoản quản trị (Admin) lần đầu tiên (email và mật khẩu tùy chọn).
4. Chọn **Add your data** hoặc vào **Admin settings** $\to$ **Databases** $\to$ **Add database**:
   * **Database type:** PostgreSQL
   * **Name:** `Grupo Bimbo Serving DB`
   * **Host:** `postgres` *(tên service trong Docker network)*
   * **Port:** `5432`
   * **Database name:** `bimbo`
   * **Database username:** `bimbo` *(lấy từ `.env`)*
   * **Database password:** `bimbo123` *(lấy từ `.env`)*
5. Nhấn **Save**. Metabase sẽ tự động đồng bộ schema `gold` cùng 4 bảng dữ liệu tổng hợp.

---

## 2. Tạo 4 Thẻ Phân tích (Visual Cards) cho Q1–Q4

Vào mục **+ New** $\to$ **SQL query**, chọn database `Grupo Bimbo Serving DB`, dán mã SQL tương ứng và chọn dạng biểu đồ trực quan:

### Card 1: Xu hướng Nhu cầu & Doanh thu theo Tuần & Kênh (Q1)
* **File SQL:** [`sql/analysis/q1_weekly_demand.sql`](../sql/analysis/q1_weekly_demand.sql)
* **Loại biểu đồ gợi ý:** **Line Chart** hoặc **Combo Chart**
  * Trục hoành (X-axis): `Semana`
  * Trục tung (Y-axis): `demand_units` hoặc `sales_pesos`
  * Phân nhóm (Group by): `Canal_ID`
* **Ý nghĩa:** Quan sát biến động lượng tiêu thụ bánh qua các tuần và tỷ trọng giữa các kênh phân phối.

### Card 2: Phát hiện Dị biệt Tỷ lệ Hoàn hàng theo Tuyến & Sản phẩm (Q2)
* **File SQL:** [`sql/analysis/q2_return_rate.sql`](../sql/analysis/q2_return_rate.sql)
* **Loại biểu đồ gợi ý:** **Table** với Conditional Formatting (Tô đỏ các hàng `is_anomaly = true`) hoặc **Scatter Plot**
  * Cột hiển thị: `product_name`, `brand`, `Town`, `State`, `sales_units`, `return_units`, `return_rate`
* **Ý nghĩa:** Cảnh báo các tuyến giao nhận có tỷ lệ trả hàng vượt ngưỡng $2\sigma$ cần điều chỉnh lượng cung ứng.

### Card 3: Top Sản phẩm & Thị phần Thương hiệu theo Bang (Q3)
* **File SQL:** [`sql/analysis/q3_product_ranking.sql`](../sql/analysis/q3_product_ranking.sql)
* **Loại biểu đồ gợi ý:** **Stacked Bar Chart** hoặc **Horizontal Bar Chart**
  * Trục tung (Y-axis): `State`
  * Trục hoành (X-axis): `revenue_share_pct` hoặc `net_revenue`
  * Phân nhóm (Color by): `brand` (mã brand tách từ tên sản phẩm, ví dụ `BIM`, `MLA`, `TR`; nên lọc top 5 brand để biểu đồ dễ đọc)
* **Ý nghĩa:** Đánh giá độ phủ và sở thích tiêu dùng theo từng bang tại Mexico.

### Card 4: Phân khúc Khách hàng Điểm bán lẻ (Q4)
* **File SQL:** [`sql/analysis/q4_client_segments.sql`](../sql/analysis/q4_client_segments.sql)
* **Loại biểu đồ gợi ý:** **Pie / Donut Chart** kết hợp **Summary Metric Card**
  * Kích thước góc (Dimension): `segment` (VIP / High-Value, Regular / Loyal, High Return Risk, Occasional / Churn Risk, Standard, Unknown / Unidentified)
  * Giá trị (Measure): `total_net_revenue` hoặc `total_clients`
* **Ý nghĩa:** Phân bổ nguồn lực chăm sóc khách hàng và chính sách chiết khấu theo phân khúc.

---

## 3. Gom vào Dashboard & Xuất Báo cáo

1. Nhấn **+ New** $\to$ **Dashboard**, đặt tên: `Grupo Bimbo – Supply Chain Analytics`.
2. Thêm cả 4 Card đã tạo vào dashboard, sắp xếp layout cân đối (2 biểu đồ trên, 2 bảng/biểu đồ dưới).
3. Chụp ảnh toàn bộ dashboard, lưu thành `docs/report/dashboard.png`, rồi chèn vào mục 5 của báo cáo và Slide 7 (thay các dòng `TODO` đang để sẵn).
