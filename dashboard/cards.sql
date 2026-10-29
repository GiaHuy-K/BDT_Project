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
