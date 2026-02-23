-- Replace with the table you loaded, e.g. yellow_2023_01
-- Quick sanity checks
SELECT COUNT(*) FROM yellow_2023_01;

-- Top pickup locations
SELECT PULocationID, COUNT(*) AS trips
FROM yellow_2023_01
GROUP BY 1
ORDER BY trips DESC
LIMIT 20;

-- Hourly demand
SELECT date_trunc('hour', tpep_pickup_datetime) AS hour, COUNT(*) AS trips
FROM yellow_2023_01
GROUP BY 1
ORDER BY 1
LIMIT 48;