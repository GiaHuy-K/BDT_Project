-- Q4: How do clients split by purchase frequency, volume and return rate?
SELECT segment,
       COUNT(*) AS total_clients,
       ROUND(SUM(net_revenue), 2) AS total_net_revenue,
       ROUND(AVG(net_revenue), 2) AS avg_revenue_per_client,
       ROUND(AVG(return_rate) * 100, 2) AS avg_return_rate_pct,
       ROUND(AVG(active_weeks), 1) AS avg_active_weeks
FROM gold.client_segments
GROUP BY segment
ORDER BY total_net_revenue DESC;
