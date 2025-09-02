-- Count total rows
SELECT COUNT(*) AS total_rows FROM main.weather;

-- Count rows by city
SELECT city, COUNT(*) AS n
FROM main.weather
GROUP BY city
ORDER BY n DESC;

SELECT city, MAX(dt) AS latest_timestamp
FROM main.weather
GROUP BY city;

SELECT city,
       ROUND(AVG(temp_c), 2) AS avg_temp,
       ROUND(AVG(rh), 2) AS avg_humidity
FROM main.weather
GROUP BY city
ORDER BY avg_temp DESC;

-- Hours with measurable rain
SELECT city, COUNT(*) AS rainy_hours
FROM main.weather
WHERE precip_mm > 0
GROUP BY city;

-- Probability of precipitation (pop_pct)
SELECT city, ROUND(AVG(pop_pct), 1) AS avg_pop_pct
FROM main.weather
GROUP BY city;

SELECT city,
       ROUND(MAX(wind_ms), 2) AS max_wind_speed,
       ROUND(AVG(wind_ms), 2) AS avg_wind_speed
FROM main.weather
GROUP BY city;

SELECT _id, COUNT(*) AS cnt
FROM main.weather
GROUP BY _id
HAVING COUNT(*) > 1;

SELECT city, dt, temp_c, uv_index
FROM main.weather
WHERE uv_index >= 8 OR temp_c >= 35
ORDER BY dt DESC;

SELECT city,
       ROUND(AVG(temp_c), 1) AS avg_temp,
       ROUND(AVG(rh), 1) AS avg_humidity,
       ROUND(AVG(precip_mm), 2) AS avg_precip
FROM main.weather
WHERE dt::DATE >= DATE '2025-08-01'
GROUP BY city
ORDER BY avg_temp DESC;