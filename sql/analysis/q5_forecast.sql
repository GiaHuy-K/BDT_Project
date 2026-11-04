-- Forecast: accuracy on week 9 (clients the model never trained on)
SELECT model, ROUND(rmsle::numeric, 4) AS rmsle, ROUND(wape_state_channel_pct::numeric, 2) AS wape_state_channel_pct
FROM gold.forecast_metrics
ORDER BY rmsle;

-- Predicted units for the rows listed in test.csv (about a third of a normal week, not national demand)
SELECT "Semana", SUM(listed_rows) AS listed_rows, SUM(gbt_units) AS gbt_units, SUM(baseline_units) AS baseline_units
FROM gold.forecast_demand_by_state_channel
GROUP BY "Semana"
ORDER BY "Semana";
