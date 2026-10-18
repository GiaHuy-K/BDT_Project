-- Q2: Which products, agencies and routes have abnormally high return rates?
-- TODO: adjust column names once spark_jobs/gold.py q2_return_rate is implemented
SELECT *
FROM gold.return_rate_by_product_agency_route
ORDER BY return_rate DESC
LIMIT 20;
