-- Seed Gold serving tables for immediate Dashboard creation and validation
CREATE SCHEMA IF NOT EXISTS gold;

-- 1. Q1: weekly_demand_by_state_channel
CREATE TABLE IF NOT EXISTS gold.weekly_demand_by_state_channel (
    "Semana" INT,
    "State" VARCHAR(100),
    "Canal_ID" INT,
    demand_units BIGINT,
    sales_pesos NUMERIC(15,2),
    return_units BIGINT,
    return_pesos NUMERIC(15,2),
    wow_demand_change_pct NUMERIC(10,2),
    wow_sales_change_pct NUMERIC(10,2)
);

TRUNCATE TABLE gold.weekly_demand_by_state_channel;

INSERT INTO gold.weekly_demand_by_state_channel VALUES
(3, 'MEXICO, D.F.', 1, 1420500, 10250000.00, 35000, 245000.00, NULL, NULL),
(4, 'MEXICO, D.F.', 1, 1450200, 10480000.00, 36200, 253000.00, 2.09, 2.24),
(5, 'MEXICO, D.F.', 1, 1410800, 10190000.00, 34800, 241000.00, -2.72, -2.77),
(6, 'MEXICO, D.F.', 1, 1435000, 10370000.00, 35500, 248000.00, 1.72, 1.77),
(7, 'MEXICO, D.F.', 1, 1462000, 10580000.00, 37000, 259000.00, 1.88, 2.03),
(8, 'MEXICO, D.F.', 1, 1480000, 10710000.00, 36800, 257000.00, 1.23, 1.23),
(9, 'MEXICO, D.F.', 1, 1495000, 10830000.00, 37200, 260000.00, 1.01, 1.12),
(3, 'MEXICO, D.F.', 2, 185000, 1950000.00, 4200, 38000.00, NULL, NULL),
(4, 'MEXICO, D.F.', 2, 192000, 2030000.00, 4400, 40000.00, 3.78, 4.10),
(5, 'MEXICO, D.F.', 2, 188000, 1990000.00, 4300, 39000.00, -2.08, -1.97),
(6, 'MEXICO, D.F.', 2, 195000, 2060000.00, 4500, 41000.00, 3.72, 3.52),
(7, 'MEXICO, D.F.', 2, 198000, 2100000.00, 4600, 42000.00, 1.54, 1.94),
(8, 'MEXICO, D.F.', 2, 201000, 2140000.00, 4700, 43000.00, 1.52, 1.90),
(9, 'MEXICO, D.F.', 2, 205000, 2180000.00, 4800, 44000.00, 1.99, 1.87),
(3, 'JALISCO', 1, 980000, 6980000.00, 24500, 168000.00, NULL, NULL),
(4, 'JALISCO', 1, 995000, 7120000.00, 25200, 172000.00, 1.53, 2.01),
(5, 'JALISCO', 1, 975000, 6950000.00, 24100, 165000.00, -2.01, -2.39),
(6, 'JALISCO', 1, 990000, 7060000.00, 24800, 170000.00, 1.54, 1.58),
(7, 'JALISCO', 1, 1015000, 7250000.00, 25900, 178000.00, 2.53, 2.69),
(8, 'JALISCO', 1, 1020000, 7300000.00, 25600, 176000.00, 0.49, 0.69),
(9, 'JALISCO', 1, 1035000, 7420000.00, 26100, 180000.00, 1.47, 1.64),
(3, 'NUEVO LEON', 1, 850000, 6150000.00, 21000, 148000.00, NULL, NULL),
(4, 'NUEVO LEON', 1, 865000, 6280000.00, 21800, 152000.00, 1.76, 2.11),
(5, 'NUEVO LEON', 1, 848000, 6120000.00, 20900, 146000.00, -1.97, -2.55),
(6, 'NUEVO LEON', 1, 860000, 6220000.00, 21500, 150000.00, 1.42, 1.63),
(7, 'NUEVO LEON', 1, 882000, 6390000.00, 22400, 156000.00, 2.56, 2.73),
(8, 'NUEVO LEON', 1, 890000, 6450000.00, 22200, 155000.00, 0.91, 0.94),
(9, 'NUEVO LEON', 1, 902000, 6550000.00, 22800, 159000.00, 1.35, 1.55);

-- 2. Q2: return_rate_by_product_agency_route
CREATE TABLE IF NOT EXISTS gold.return_rate_by_product_agency_route (
    "Producto_ID" INT,
    product_name VARCHAR(255),
    brand VARCHAR(100),
    "Agencia_ID" INT,
    "Town" VARCHAR(100),
    "State" VARCHAR(100),
    "Ruta_SAK" INT,
    sales_units BIGINT,
    sales_pesos NUMERIC(15,2),
    return_units BIGINT,
    return_pesos NUMERIC(15,2),
    return_rate NUMERIC(10,4),
    return_rate_pesos NUMERIC(10,4),
    threshold_2sigma NUMERIC(10,4),
    is_anomaly BOOLEAN
);

TRUNCATE TABLE gold.return_rate_by_product_agency_route;

INSERT INTO gold.return_rate_by_product_agency_route VALUES
(1278, 'Pan Blanco Bimbo 680g', 'BIMBO', 2011, 'AG. TIJUANA NORTE', 'BAJA CALIFORNIA NORTE', 1421, 520, 14560.00, 185, 5180.00, 0.3558, 0.3558, 0.1245, true),
(2233, 'Submarinos Fresa Marinela 3p 105g', 'MARINELA', 1912, 'AG. CHIHUAHUA SUR', 'CHIHUAHUA', 2314, 410, 6150.00, 142, 2130.00, 0.3463, 0.3463, 0.1245, true),
(3562, 'Donas Espolvoreadas Bimbo 6p 150g', 'BIMBO', 1614, 'AG. GUADALAJARA ORIENTE', 'JALISCO', 3105, 680, 12240.00, 220, 3960.00, 0.3235, 0.3235, 0.1245, true),
(4128, 'Mantecadas Vainilla 4p 125g', 'BIMBO', 2210, 'AG. MERIDA CENTRO', 'YUCATAN', 1102, 380, 6840.00, 118, 2124.00, 0.3105, 0.3105, 0.1245, true),
(5214, 'Tortillinas Tia Rosa 12p 310g', 'TIA ROSA', 1120, 'AG. PUEBLA NORTE', 'PUEBLA', 4022, 750, 13500.00, 215, 3870.00, 0.2867, 0.2867, 0.1245, true),
(1278, 'Pan Blanco Bimbo 680g', 'BIMBO', 1010, 'AG. IZTAPALAPA', 'MEXICO, D.F.', 1050, 12400, 347200.00, 480, 13440.00, 0.0387, 0.0387, 0.1245, false),
(2233, 'Submarinos Fresa Marinela 3p 105g', 'MARINELA', 1010, 'AG. IZTAPALAPA', 'MEXICO, D.F.', 1050, 8500, 127500.00, 310, 4650.00, 0.0365, 0.0365, 0.1245, false),
(3562, 'Donas Espolvoreadas Bimbo 6p 150g', 'BIMBO', 1010, 'AG. IZTAPALAPA', 'MEXICO, D.F.', 1050, 9200, 165600.00, 380, 6840.00, 0.0413, 0.0413, 0.1245, false);

-- 3. Q3: product_ranking_by_state
CREATE TABLE IF NOT EXISTS gold.product_ranking_by_state (
    "State" VARCHAR(100),
    rank INT,
    "Producto_ID" INT,
    product_name VARCHAR(255),
    brand VARCHAR(100),
    net_revenue NUMERIC(15,2),
    sales_pesos NUMERIC(15,2),
    sales_units BIGINT,
    return_units BIGINT,
    revenue_share_pct NUMERIC(10,2),
    state_net_revenue NUMERIC(15,2)
);

TRUNCATE TABLE gold.product_ranking_by_state;

INSERT INTO gold.product_ranking_by_state VALUES
('MEXICO, D.F.', 1, 1278, 'Pan Blanco Bimbo 680g', 'BIMBO', 14500000.00, 14950000.00, 535000, 16100, 16.85, 86050000.00),
('MEXICO, D.F.', 2, 1146, 'Pan Integral Bimbo 680g', 'BIMBO', 11200000.00, 11520000.00, 410000, 11400, 13.02, 86050000.00),
('MEXICO, D.F.', 3, 2233, 'Submarinos Fresa Marinela 3p 105g', 'MARINELA', 8900000.00, 9150000.00, 595000, 16600, 10.34, 86050000.00),
('MEXICO, D.F.', 4, 3562, 'Donas Espolvoreadas Bimbo 6p 150g', 'BIMBO', 7400000.00, 7620000.00, 412000, 12200, 8.60, 86050000.00),
('MEXICO, D.F.', 5, 5214, 'Tortillinas Tia Rosa 12p 310g', 'TIA ROSA', 6800000.00, 6980000.00, 378000, 9700, 7.90, 86050000.00),
('MEXICO, D.F.', 6, 4128, 'Mantecadas Vainilla 4p 125g', 'BIMBO', 5900000.00, 6080000.00, 328000, 9700, 6.86, 86050000.00),
('MEXICO, D.F.', 7, 3054, 'Barritas Fresa Marinela 2p 67g', 'MARINELA', 4850000.00, 4980000.00, 495000, 12900, 5.64, 86050000.00),
('MEXICO, D.F.', 8, 1210, 'Medias Noches Bimbo 8p 340g', 'BIMBO', 4400000.00, 4520000.00, 245000, 6500, 5.11, 86050000.00),
('MEXICO, D.F.', 9, 3180, 'Gansito Marinela 1p 50g', 'MARINELA', 4100000.00, 4210000.00, 455000, 11900, 4.76, 86050000.00),
('MEXICO, D.F.', 10, 6022, 'Polvorones Marinela 6p 113g', 'MARINELA', 3600000.00, 3700000.00, 310000, 8400, 4.18, 86050000.00),
('JALISCO', 1, 1278, 'Pan Blanco Bimbo 680g', 'BIMBO', 9800000.00, 10100000.00, 362000, 10800, 17.15, 57150000.00),
('JALISCO', 2, 5214, 'Tortillinas Tia Rosa 12p 310g', 'TIA ROSA', 8100000.00, 8320000.00, 450000, 11900, 14.17, 57150000.00),
('JALISCO', 3, 1146, 'Pan Integral Bimbo 680g', 'BIMBO', 7400000.00, 7610000.00, 272000, 7500, 12.95, 57150000.00),
('JALISCO', 4, 2233, 'Submarinos Fresa Marinela 3p 105g', 'MARINELA', 5800000.00, 5970000.00, 388000, 11100, 10.15, 57150000.00),
('JALISCO', 5, 3562, 'Donas Espolvoreadas Bimbo 6p 150g', 'BIMBO', 4950000.00, 5100000.00, 275000, 8100, 8.66, 57150000.00);

-- 4. Q4: client_segments
CREATE TABLE IF NOT EXISTS gold.client_segments (
    "Cliente_ID" INT,
    client_name VARCHAR(255),
    is_unknown_client BOOLEAN,
    active_weeks INT,
    total_transactions INT,
    demand_units BIGINT,
    sales_pesos NUMERIC(15,2),
    return_units BIGINT,
    return_pesos NUMERIC(15,2),
    net_revenue NUMERIC(15,2),
    return_rate NUMERIC(10,4),
    segment VARCHAR(100)
);

TRUNCATE TABLE gold.client_segments;

INSERT INTO gold.client_segments VALUES
(1001, 'ABARROTES LA GUADALUPANA', false, 7, 245, 3850, 68500.00, 42, 740.00, 67760.00, 0.0108, 'VIP / High-Value'),
(1002, 'TIENDA DON PEPE', false, 7, 198, 2980, 53200.00, 35, 620.00, 52580.00, 0.0116, 'VIP / High-Value'),
(1003, 'SUPER EXPRESS EL AGUILA', false, 6, 175, 2650, 47800.00, 28, 500.00, 47300.00, 0.0105, 'VIP / High-Value'),
(1004, 'MISCELANEA SAN RAFAEL', false, 5, 95, 1120, 18900.00, 18, 305.00, 18595.00, 0.0158, 'Regular / Loyal'),
(1005, 'ABARROTES LOS PRIMOS', false, 6, 110, 1280, 21500.00, 22, 370.00, 21130.00, 0.0169, 'Regular / Loyal'),
(1006, 'DEPOSITO EL SOL', false, 4, 82, 940, 15600.00, 14, 230.00, 15370.00, 0.0147, 'Regular / Loyal'),
(1007, 'MINI SUPER LAS PALMAS', false, 5, 120, 1450, 24200.00, 320, 5340.00, 18860.00, 0.1808, 'High Return Risk'),
(1008, 'TIENDA LA ESTRELLA', false, 4, 88, 980, 16400.00, 210, 3510.00, 12890.00, 0.1765, 'High Return Risk'),
(1009, 'PUESTO DE PERIODICOS EL CENTRO', false, 2, 18, 140, 2350.00, 2, 34.00, 2316.00, 0.0141, 'Occasional / Churn Risk'),
(1010, 'FONDA DONA LUPE', false, 1, 8, 65, 1100.00, 0, 0.00, 1100.00, 0.0000, 'Occasional / Churn Risk'),
(1011, 'UNKNOWN', true, 4, 65, 780, 12800.00, 12, 195.00, 12605.00, 0.0152, 'Unknown / Unidentified');
