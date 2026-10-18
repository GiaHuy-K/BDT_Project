-- Q1: How does demand change by week, state and sales channel?
SELECT "Semana", "State", "Canal_ID", demand_units, sales_pesos
FROM gold.weekly_demand_by_state_channel
ORDER BY "State", "Canal_ID", "Semana";
