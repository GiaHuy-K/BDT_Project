# Dashboard (Metabase)

1. Mở http://localhost:3000, tạo tài khoản admin lần đầu.
2. Add database → PostgreSQL: host `postgres`, port `5432`, database `bimbo`, user/password lấy từ `.env`.
3. Tạo một câu hỏi (SQL) cho mỗi file trong [`sql/analysis/`](../sql/analysis), gom vào dashboard "Grupo Bimbo – Q1–Q4".
4. Export dashboard (ảnh/PDF) vào `docs/report/` làm bằng chứng kết quả phân tích, kèm diễn giải.
