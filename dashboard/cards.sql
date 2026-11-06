-- Dashboard-ready queries: one card = one query = one measure, already aggregated.
-- Paste each block into Metabase (+ New > SQL query, database "Grupo Bimbo Serving DB").
-- Spark writes DOUBLE PRECISION columns; cast to numeric before ROUND(x, n).

-- Card 1a (Q1) · Line: national demand per week, million units
SELECT "Semana" AS tuan,
       ROUND(SUM(demand_units) / 1000000.0, 2) AS nhu_cau_trieu_don_vi
FROM gold.weekly_demand_by_state_channel
GROUP BY "Semana"
ORDER BY "Semana";

-- Card 1b (Q1) · Line: returns as % of demand per week
SELECT "Semana" AS tuan,
       ROUND(100.0 * SUM(return_units) / SUM(demand_units), 2) AS ty_le_tra_pct
FROM gold.weekly_demand_by_state_channel
GROUP BY "Semana"
ORDER BY "Semana";

-- Card 2a (Q2) · Bar: share held by the anomalous combinations, %
SELECT 'Số tổ hợp' AS chi_so,
       ROUND(100.0 * COUNT(*) FILTER (WHERE is_anomaly)
             / COUNT(*) FILTER (WHERE sales_units >= 20), 2) AS ty_trong_pct
FROM gold.return_rate_by_product_agency_route
UNION ALL
SELECT 'Doanh thu',
       ROUND((100.0 * SUM(sales_pesos) FILTER (WHERE is_anomaly) / SUM(sales_pesos))::numeric, 2)
FROM gold.return_rate_by_product_agency_route
UNION ALL
SELECT 'Giá trị hàng trả',
       ROUND((100.0 * SUM(return_pesos) FILTER (WHERE is_anomaly) / SUM(return_pesos))::numeric, 2)
FROM gold.return_rate_by_product_agency_route;

-- Card 2a, highlighted version · Row chart, 2 series, Stack ON:
-- "noi_bat" (returned value) gets its own colour, the other bars share one.
WITH s AS (
  SELECT 1 AS thu_tu, 'Số tổ hợp' AS chi_so,
         ROUND(100.0 * COUNT(*) FILTER (WHERE is_anomaly) / COUNT(*) FILTER (WHERE sales_units >= 20), 2) AS pct
  FROM gold.return_rate_by_product_agency_route
  UNION ALL
  SELECT 2, 'Doanh thu',
         ROUND((100.0 * SUM(sales_pesos) FILTER (WHERE is_anomaly) / SUM(sales_pesos))::numeric, 2)
  FROM gold.return_rate_by_product_agency_route
  UNION ALL
  SELECT 3, 'Giá trị hàng trả',
         ROUND((100.0 * SUM(return_pesos) FILTER (WHERE is_anomaly) / SUM(return_pesos))::numeric, 2)
  FROM gold.return_rate_by_product_agency_route
)
SELECT chi_so,
       CASE WHEN chi_so <> 'Giá trị hàng trả' THEN pct END AS binh_thuong,
       CASE WHEN chi_so =  'Giá trị hàng trả' THEN pct END AS noi_bat
FROM s
ORDER BY thu_tu;

-- Card 2b (Q2) · Table: top 15 anomalous combinations by returned value
SELECT product_name AS san_pham, "Town" AS agency, "State" AS bang, "Ruta_SAK" AS tuyen,
       sales_units AS ban, return_units AS tra,
       ROUND((return_rate * 100)::numeric, 1) AS ty_le_tra_pct,
       ROUND(return_pesos::numeric, 0) AS gia_tri_tra_peso
FROM gold.return_rate_by_product_agency_route
WHERE is_anomaly
ORDER BY return_pesos DESC
LIMIT 15;

-- Card 3 (Q3) · Row chart: net revenue share by brand, top 8, %
SELECT brand,
       ROUND((100.0 * SUM(net_revenue) / SUM(SUM(net_revenue)) OVER ())::numeric, 2) AS thi_phan_pct
FROM gold.product_ranking_by_state
GROUP BY brand
ORDER BY thi_phan_pct DESC
LIMIT 8;

-- Card 4 (Q4) · Bar, 2 series (both %): store share vs revenue share per segment
SELECT segment AS phan_khuc,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_diem_ban,
       ROUND((100.0 * SUM(net_revenue) / SUM(SUM(net_revenue)) OVER ())::numeric, 2) AS pct_doanh_thu
FROM gold.client_segments
GROUP BY segment
ORDER BY pct_doanh_thu DESC;

-- =====================================================================
-- KPI row · 4 Number cards at the top of the dashboard (weeks 3-9)
-- =====================================================================

-- KPI 1 · Number: total demand, million units
SELECT ROUND(SUM(demand_units) / 1000000.0, 1) AS nhu_cau_trieu_don_vi
FROM gold.weekly_demand_by_state_channel;

-- KPI 2 · Number: net revenue (sales - returned value), million pesos
SELECT ROUND((SUM(sales_pesos - return_pesos) / 1000000.0)::numeric, 1) AS doanh_thu_thuan_trieu_peso
FROM gold.weekly_demand_by_state_channel;

-- KPI 3 · Number: returned value as % of sales value
SELECT ROUND((100.0 * SUM(return_pesos) / SUM(sales_pesos))::numeric, 2) AS ty_le_tra_gia_tri_pct
FROM gold.weekly_demand_by_state_channel;

-- KPI 4 · Number: points of sale with at least one delivery
SELECT COUNT(*) AS so_diem_ban
FROM gold.client_segments;

-- =====================================================================
-- Detail tables
-- =====================================================================

-- Table A · By state: demand, net revenue, share of national revenue, return rate
SELECT "State" AS bang,
       SUM(demand_units) AS nhu_cau,
       ROUND((SUM(sales_pesos - return_pesos) / 1000000.0)::numeric, 2) AS doanh_thu_thuan_trieu_peso,
       ROUND((100.0 * SUM(sales_pesos - return_pesos)
              / SUM(SUM(sales_pesos - return_pesos)) OVER ())::numeric, 2) AS pct_doanh_thu,
       ROUND((100.0 * SUM(return_pesos) / SUM(sales_pesos))::numeric, 2) AS ty_le_tra_pct
FROM gold.weekly_demand_by_state_channel
GROUP BY "State"
ORDER BY doanh_thu_thuan_trieu_peso DESC;

-- Table B · By channel: share of rows vs share of revenue, return rate
SELECT w."Canal_ID" AS kenh,
       ROUND(100.0 * o.rows / SUM(o.rows) OVER (), 2) AS pct_so_dong,
       ROUND((100.0 * w.net / SUM(w.net) OVER ())::numeric, 2) AS pct_doanh_thu,
       ROUND((100.0 * w.ret / w.sales)::numeric, 2) AS ty_le_tra_pct
FROM (SELECT "Canal_ID", SUM(sales_pesos - return_pesos) AS net,
             SUM(return_pesos) AS ret, SUM(sales_pesos) AS sales
      FROM gold.weekly_demand_by_state_channel GROUP BY "Canal_ID") w
JOIN gold.outliers_by_channel o USING ("Canal_ID")
ORDER BY pct_doanh_thu DESC;

-- Table C · Best-selling product in each state (rank 1 by net revenue)
SELECT "State" AS bang, product_name AS san_pham, brand,
       ROUND(net_revenue::numeric, 0) AS doanh_thu_thuan_peso,
       revenue_share_pct AS pct_doanh_thu_bang
FROM gold.product_ranking_by_state
WHERE rank = 1
ORDER BY net_revenue DESC;

-- Table D · Outliers: one global threshold vs each channel's own threshold
SELECT "Canal_ID" AS kenh, rows AS so_dong,
       ROUND(channel_threshold::numeric, 0) AS nguong_theo_kenh,
       flagged_global AS co_nguong_chung, flagged_global_pct AS pct_nguong_chung,
       flagged_channel AS co_nguong_kenh, flagged_channel_pct AS pct_nguong_kenh
FROM gold.outliers_by_channel
ORDER BY rows DESC;

-- Table E · Forecast accuracy on week 9 (clients the model never saw)
SELECT CASE model WHEN 'gbt' THEN 'GBT (Spark MLlib)'
                  WHEN 'history_baseline' THEN 'Trung bình lịch sử'
                  WHEN 'product_baseline' THEN 'Trung bình theo sản phẩm' END AS mo_hinh,
       ROUND(rmsle::numeric, 3) AS rmsle_tung_dong,
       ROUND(wape_state_channel_pct::numeric, 1) AS sai_so_bang_kenh_pct
FROM gold.forecast_metrics
ORDER BY rmsle;

-- Table F · Forecast for weeks 10-11 by state (only the rows listed in test.csv,
-- about a third of a normal week, so these are not national totals)
SELECT "State" AS bang,
       SUM(gbt_units) FILTER (WHERE "Semana" = 10) AS gbt_tuan_10,
       SUM(gbt_units) FILTER (WHERE "Semana" = 11) AS gbt_tuan_11,
       SUM(baseline_units) FILTER (WHERE "Semana" = 10) AS baseline_tuan_10,
       SUM(baseline_units) FILTER (WHERE "Semana" = 11) AS baseline_tuan_11
FROM gold.forecast_demand_by_state_channel
GROUP BY "State"
ORDER BY gbt_tuan_10 DESC NULLS LAST;
