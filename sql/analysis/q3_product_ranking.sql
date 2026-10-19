-- Q3: Which products and brands lead revenue in each state?
SELECT "State", rank, "Producto_ID", product_name, brand,
       net_revenue, sales_pesos, sales_units, return_units,
       revenue_share_pct, state_net_revenue
FROM gold.product_ranking_by_state
WHERE rank <= 10
ORDER BY "State", rank;
