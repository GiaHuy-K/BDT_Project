-- Q4: How do clients split by purchase frequency, volume and return rate?
-- TODO: adjust column names once spark_jobs/gold.py q4_client_segments is implemented
SELECT segment, COUNT(*) AS clients, SUM(net_revenue) AS net_revenue
FROM gold.client_segments
GROUP BY segment
ORDER BY net_revenue DESC;
