-- Q4: How do clients split by purchase frequency, volume and return rate?
-- Spark writes DOUBLE PRECISION columns; cast to numeric before ROUND(x, n).
SELECT segment,
       COUNT(*) AS total_clients,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS client_share_pct,
       ROUND(SUM(net_revenue)::numeric, 2) AS total_net_revenue,
       ROUND((100.0 * SUM(net_revenue) / SUM(SUM(net_revenue)) OVER ())::numeric, 2) AS revenue_share_pct,
       ROUND(AVG(net_revenue)::numeric, 2) AS avg_revenue_per_client,
       ROUND((100.0 * SUM(return_pesos) / NULLIF(SUM(sales_pesos), 0))::numeric, 2) AS return_rate_pesos_pct,
       ROUND(AVG(active_weeks)::numeric, 1) AS avg_active_weeks
FROM gold.client_segments
GROUP BY segment
ORDER BY total_net_revenue DESC;
