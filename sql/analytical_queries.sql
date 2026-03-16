-- sql/analytical_queries.sql
-- Business insight queries against the analytics mart
-- Run these in DBeaver, Azure Data Studio, or any SQLite client

-- ── 1. Top weather × category correlations ────────────────────
-- Which app categories spike most on rainy days?
SELECT
    weather_label,
    category,
    avg_rating,
    ROUND(avg_install_index, 3)         AS install_index,
    pct_vs_baseline || '%'              AS vs_baseline,
    insight_label
FROM mart_weather_app_insights
ORDER BY ABS(pct_vs_baseline) DESC
LIMIT 20;


-- ── 2. Rainy day winners ───────────────────────────────────────
SELECT
    category,
    avg_rating,
    ROUND(avg_install_index, 3)         AS install_index,
    pct_vs_baseline                     AS pct_above_baseline
FROM mart_weather_app_insights
WHERE weather_label = 'Rainy'
ORDER BY pct_vs_baseline DESC
LIMIT 10;


-- ── 3. Sunny day winners ──────────────────────────────────────
SELECT
    category,
    avg_rating,
    ROUND(avg_install_index, 3)         AS install_index,
    pct_vs_baseline
FROM mart_weather_app_insights
WHERE weather_label = 'Sunny'
ORDER BY pct_vs_baseline DESC
LIMIT 10;


-- ── 4. Daily pipeline trend — rows loaded per day ─────────────
SELECT
    DATE(completed_at)                  AS run_date,
    COUNT(*)                            AS pipeline_runs,
    SUM(rows_processed)                 AS total_rows,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successful_stages,
    SUM(CASE WHEN status = 'failed'  THEN 1 ELSE 0 END) AS failed_stages
FROM pipeline_log
GROUP BY run_date
ORDER BY run_date DESC;


-- ── 5. Weather distribution across tracked cities ─────────────
SELECT
    city,
    weather_label,
    COUNT(*)                            AS days,
    ROUND(AVG(avg_temp_c), 1)          AS avg_temp_c,
    ROUND(AVG(total_precip_mm), 2)     AS avg_precip_mm
FROM stg_weather
GROUP BY city, weather_label
ORDER BY city, days DESC;


-- ── 6. Category performance heatmap data ──────────────────────
-- Pivot-ready output for Power BI / Tableau
SELECT
    category,
    ROUND(AVG(CASE WHEN weather_label = 'Sunny'  THEN pct_vs_baseline END), 1) AS sunny_pct,
    ROUND(AVG(CASE WHEN weather_label = 'Rainy'  THEN pct_vs_baseline END), 1) AS rainy_pct,
    ROUND(AVG(CASE WHEN weather_label = 'Cloudy' THEN pct_vs_baseline END), 1) AS cloudy_pct,
    ROUND(AVG(CASE WHEN weather_label = 'Snowy'  THEN pct_vs_baseline END), 1) AS snowy_pct,
    ROUND(AVG(CASE WHEN weather_label = 'Stormy' THEN pct_vs_baseline END), 1) AS stormy_pct
FROM mart_weather_app_insights
GROUP BY category
ORDER BY rainy_pct DESC NULLS LAST;
