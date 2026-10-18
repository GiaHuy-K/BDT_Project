# Dataset

Nguồn: [Kaggle – Grupo Bimbo Inventory Demand](https://www.kaggle.com/competitions/grupo-bimbo-inventory-demand/data), dùng theo điều khoản cuộc thi, chỉ cho mục đích học tập.

| File | Vai trò | Kích thước | Số dòng |
| --- | --- | --- | --- |
| `train.csv` | Fact: giao hàng tuần 3–9 | 3,199 MB | 74,180,464 |
| `test.csv` | Fact: tuần 10–11, không có nhu cầu | 251 MB | 6,999,251 |
| `cliente_tabla.csv` | Dimension: khách hàng | 21 MB | 935,362 |
| `producto_tabla.csv` | Dimension: sản phẩm | 0.1 MB | 2,592 |
| `town_state.csv` | Dimension: agency → thành phố, bang | < 0.1 MB | 790 |
| `sample_submission.csv` | Format nộp Kaggle – không ingest | 69 MB | – |

## Vấn đề chất lượng

| Vấn đề | Mức độ | Xử lý |
| --- | --- | --- |
| `Cliente_ID` trùng trong `cliente_tabla` | 4,862 ID | Dedup ở silver |
| Khách hàng "SIN NOMBRE" | 379 dòng | Gắn nhãn unknown |
| Tên sản phẩm gộp nhiều thông tin | 2,592 sản phẩm | Regex tách pieces, weight, brand |
| Tên bang không thống nhất, có dấu | – | Đọc UTF-8, chuẩn hóa |
| Dòng có hàng trả lại | 2.54 triệu (3.4%) | Dùng cho Q2 |
| Nhu cầu = 0 | 1.34 triệu (1.8%) | Giữ, đánh dấu |
| Demand lệch mạnh (mean 7.2, max 5,000) | – | Cờ outlier, không xóa |
| Kênh 1 chiếm ~91% dòng | 67.4 / 74.2 triệu | Lưu ý skew khi join |
