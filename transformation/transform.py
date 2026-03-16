# transformation/transform.py
# Cleans raw data, classifies weather, aggregates app metrics,
# and builds the daily fact table joining weather + app categories

import logging
import sqlite3

logger = logging.getLogger(__name__)


# WMO weather code → human label mapping
# https://open-meteo.com/en/docs#weathervariables
WMO_LABELS = {
    range(0,   1):  "Sunny",
    range(1,   4):  "Partly Cloudy",
    range(45,  50): "Foggy",
    range(51,  68): "Rainy",
    range(71,  78): "Snowy",
    range(80,  83): "Rainy",       # rain showers
    range(85,  87): "Snowy",       # snow showers
    range(95,  100): "Stormy",
}


def classify_weather(code: int | None, precip_mm: float | None) -> dict:
    """
    Returns weather_label, is_rainy, is_sunny based on WMO code + precip.
    Falls back to precip-based classification if code is null.
    """
    label = "Unknown"

    if code is not None:
        for code_range, lbl in WMO_LABELS.items():
            if code in code_range:
                label = lbl
                break

    # Override with precip if label is still ambiguous
    if label in ("Unknown", "Partly Cloudy") and precip_mm is not None:
        if precip_mm > 2.0:
            label = "Rainy"
        elif precip_mm == 0:
            label = "Sunny"
        else:
            label = "Cloudy"

    return {
        "weather_label": label,
        "is_rainy":      1 if label in ("Rainy", "Stormy") else 0,
        "is_sunny":      1 if label == "Sunny" else 0,
    }


def transform_weather(conn: sqlite3.Connection) -> int:
    """
    Aggregates raw hourly weather to daily level, classifies weather type.
    Writes to stg_weather — skips dates already processed (incremental).
    """
    cursor = conn.cursor()

    # Find city+date combos in raw not yet in stg
    cursor.execute("""
        SELECT r.city, r.date
        FROM raw_weather r
        LEFT JOIN stg_weather s ON r.city = s.city AND r.date = s.date
        WHERE s.id IS NULL
        GROUP BY r.city, r.date
    """)
    pending = cursor.fetchall()

    if not pending:
        logger.info("transform_weather: nothing new to process")
        return 0

    inserted = 0
    for city, date in pending:
        cursor.execute("""
            SELECT
                AVG(temperature_c),
                SUM(precipitation_mm),
                AVG(windspeed_kmh),
                AVG(cloudcover_pct),
                -- dominant weathercode = most frequent non-null code that day
                (
                    SELECT weathercode
                    FROM raw_weather
                    WHERE city = ? AND date = ? AND weathercode IS NOT NULL
                    GROUP BY weathercode
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                )
            FROM raw_weather
            WHERE city = ? AND date = ?
        """, (city, date, city, date))

        row = cursor.fetchone()
        if not row:
            continue

        avg_temp, total_precip, avg_wind, avg_cloud, dominant_code = row
        classification = classify_weather(dominant_code, total_precip)

        cursor.execute("""
            INSERT OR IGNORE INTO stg_weather
                (city, date, avg_temp_c, total_precip_mm, avg_windspeed,
                 avg_cloudcover, dominant_code, weather_label, is_rainy, is_sunny)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            city, date,
            round(avg_temp, 2)   if avg_temp   is not None else None,
            round(total_precip, 2) if total_precip is not None else None,
            round(avg_wind, 2)   if avg_wind   is not None else None,
            round(avg_cloud, 2)  if avg_cloud  is not None else None,
            dominant_code,
            classification["weather_label"],
            classification["is_rainy"],
            classification["is_sunny"],
        ))
        inserted += 1

    conn.commit()
    logger.info(f"transform_weather: {inserted} city-date records written to stg_weather")
    return inserted


def transform_apps(conn: sqlite3.Connection) -> int:
    """
    Cleans raw_apps and writes to stg_apps.
    One-time transform — skips if stg_apps already populated.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stg_apps")
    if cursor.fetchone()[0] > 0:
        logger.info("transform_apps: stg_apps already populated — skipping")
        return 0

    cursor.execute("""
        INSERT INTO stg_apps (app, category, rating, reviews, installs, is_free, price)
        SELECT
            app,
            UPPER(TRIM(category))           AS category,
            CASE
                WHEN rating BETWEEN 1.0 AND 5.0 THEN rating
                ELSE NULL
            END                             AS rating,
            CASE WHEN reviews > 0 THEN reviews ELSE NULL END AS reviews,
            CASE WHEN installs > 0 THEN installs ELSE NULL END AS installs,
            CASE WHEN UPPER(type) = 'FREE' OR price = 0 THEN 1 ELSE 0 END AS is_free,
            COALESCE(price, 0.0)            AS price
        FROM raw_apps
        WHERE app IS NOT NULL AND app != ''
        GROUP BY app   -- deduplicate
        HAVING installs = MAX(installs)
    """)

    conn.commit()
    count = cursor.rowcount
    logger.info(f"transform_apps: {count} apps written to stg_apps")
    return count


def build_fact_table(conn: sqlite3.Connection) -> int:
    """
    Joins stg_weather × stg_apps categories to build daily fact table.
    Computes install_index = category installs / overall avg installs.
    Incremental — skips city+date+category combos already in fact table.
    """
    cursor = conn.cursor()

    # Compute overall avg installs as baseline
    cursor.execute("SELECT AVG(installs) FROM stg_apps WHERE installs IS NOT NULL")
    baseline_row = cursor.fetchone()
    baseline_installs = baseline_row[0] if baseline_row and baseline_row[0] else 1

    # Get pending weather dates
    cursor.execute("""
        SELECT w.city, w.date, w.weather_label, w.avg_temp_c, w.total_precip_mm
        FROM stg_weather w
        WHERE NOT EXISTS (
            SELECT 1 FROM fct_weather_app_daily f
            WHERE f.city = w.city AND f.date = w.date
        )
    """)
    pending_weather = cursor.fetchall()

    if not pending_weather:
        logger.info("build_fact_table: nothing new to process")
        return 0

    # Get category aggregates (static — computed once from stg_apps)
    cursor.execute("""
        SELECT
            category,
            ROUND(AVG(rating), 3)       AS avg_rating,
            SUM(installs)               AS total_installs,
            COUNT(*)                    AS app_count
        FROM stg_apps
        WHERE category IS NOT NULL AND installs IS NOT NULL
        GROUP BY category
    """)
    category_stats = {r[0]: r for r in cursor.fetchall()}

    rows_inserted = 0
    for city, date, weather_label, avg_temp, total_precip in pending_weather:
        for category, stats in category_stats.items():
            _, avg_rating, total_installs, app_count = stats
            install_index = round(
                (total_installs / app_count) / baseline_installs, 4
            ) if app_count and baseline_installs else None

            cursor.execute("""
                INSERT OR IGNORE INTO fct_weather_app_daily
                    (city, date, category, weather_label, avg_temp_c,
                     total_precip_mm, category_avg_rating,
                     category_total_installs, category_app_count, install_index)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                city, date, category, weather_label, avg_temp,
                total_precip, avg_rating, total_installs,
                app_count, install_index
            ))
            rows_inserted += 1

    conn.commit()
    logger.info(f"build_fact_table: {rows_inserted} rows written to fct_weather_app_daily")
    return rows_inserted
