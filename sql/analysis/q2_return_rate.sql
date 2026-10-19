-- Q2: Which products, agencies and routes have abnormally high return rates?
SELECT "Producto_ID", product_name, brand, "Agencia_ID", "Town", "State", "Ruta_SAK",
       sales_units, sales_pesos, return_units, return_pesos, return_rate,
       threshold_2sigma, is_anomaly
FROM gold.return_rate_by_product_agency_route
WHERE is_anomaly = true
ORDER BY return_rate DESC
LIMIT 50;
