-- Numbers quoted in docs/report/BDT301_Project_Report.md section 5 (one query per -- name: block).
-- notebooks/report_figures.py runs the blocks marked "figure" and draws docs/report/figures/*.png.
-- Return share = return_units / (demand_units + return_units), i.e. returns over units delivered.

-- name: q1_weekly (figure)
SELECT "Semana", SUM(demand_units) AS demand_units, ROUND(SUM(sales_pesos)::numeric) AS sales_pesos
FROM gold.weekly_demand_by_state_channel
GROUP BY 1 ORDER BY 1;

-- name: q1_channels
SELECT "Canal_ID",
       SUM(demand_units) AS demand_units,
       ROUND(100.0 * SUM(demand_units) / SUM(SUM(demand_units)) OVER (), 1) AS pct_demand,
       ROUND((SUM(sales_pesos) / NULLIF(SUM(demand_units), 0))::numeric, 2) AS pesos_per_unit,
       ROUND(100.0 * SUM(return_units) / NULLIF(SUM(demand_units) + SUM(return_units), 0), 2) AS return_pct
FROM gold.weekly_demand_by_state_channel
GROUP BY 1 ORDER BY 2 DESC;

-- name: q2_summary
SELECT MAX(threshold_2sigma) AS threshold,
       COUNT(*) FILTER (WHERE sales_units >= 20) AS eligible_groups,
       COUNT(*) FILTER (WHERE is_anomaly) AS anomalies,
       ROUND((100 * AVG(return_rate) FILTER (WHERE sales_units >= 20))::numeric, 2) AS mean_return_rate_pct,
       ROUND((100.0 * SUM(return_pesos) FILTER (WHERE is_anomaly) / SUM(return_pesos))::numeric, 1) AS pct_return_pesos_in_anomalies
FROM gold.return_rate_by_product_agency_route;

-- name: q2_by_brand (figure)
SELECT brand, COUNT(*) AS anomalies
FROM gold.return_rate_by_product_agency_route
WHERE is_anomaly
GROUP BY 1 ORDER BY 2 DESC LIMIT 8;

-- name: q3_brand_share (figure)
SELECT brand, ROUND((100.0 * SUM(net_revenue) / SUM(SUM(net_revenue)) OVER ())::numeric, 1) AS pct_net_revenue
FROM gold.product_ranking_by_state
GROUP BY 1 ORDER BY 2 DESC LIMIT 6;

-- name: q3_leaders
SELECT product_name,
       COUNT(*) FILTER (WHERE rank = 1) AS states_rank1,
       COUNT(*) FILTER (WHERE rank <= 3) AS states_top3
FROM gold.product_ranking_by_state
GROUP BY 1 HAVING COUNT(*) FILTER (WHERE rank <= 3) > 0
ORDER BY 3 DESC, 2 DESC LIMIT 6;

-- name: q4_segments (figure)
SELECT segment,
       COUNT(*) AS clients,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_clients,
       ROUND((100.0 * SUM(net_revenue) / SUM(SUM(net_revenue)) OVER ())::numeric, 1) AS pct_net_revenue,
       ROUND((100.0 * SUM(return_units) / NULLIF(SUM(demand_units) + SUM(return_units), 0))::numeric, 2) AS return_pct
FROM gold.client_segments
GROUP BY 1 ORDER BY pct_net_revenue DESC;
