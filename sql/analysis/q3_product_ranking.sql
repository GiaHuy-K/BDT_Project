-- Q3: Which products and brands lead revenue in each state?
-- TODO: adjust column names once spark_jobs/gold.py q3_product_ranking is implemented
SELECT *
FROM gold.product_ranking_by_state
WHERE rank <= 10
ORDER BY "State", rank;
