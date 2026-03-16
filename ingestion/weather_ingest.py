# ingestion/weather_ingest.py
# Pulls hourly weather data from Open-Meteo API (free, no API key needed)
# Writes raw records to raw_weather table

import logging
import sqlite3
from datetime import datetime, timedelta

import requests

from config.settings import (
    CITIES, DB_PATH, WEATHER_API_BASE,
    WEATHER_PARAMS, WEATHER_LOOKBACK_DAYS
)

logger = logging.getLogger(__name__)


def fetch_weather(city: dict, days_back: int = WEATHER_LOOKBACK_DAYS) -> list[dict]:
    """
    Fetch hourly weather for a city from Open-Meteo.
    Returns a list of row dicts ready for insertion.
    """
    end_date   = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days_back)

    params = {
        "latitude":        city["lat"],
        "longitude":       city["lon"],
        "hourly":          ",".join(WEATHER_PARAMS),
        "start_date":      str(start_date),
        "end_date":        str(end_date),
        "timezone":        "UTC",
        "temperature_unit": "celsius",
        "windspeed_unit":  "kmh",
    }

    logger.info(f"Fetching weather for {city['name']} ({start_date} → {end_date})")
    response = requests.get(WEATHER_API_BASE, params=params, timeout=15)
    response.raise_for_status()

    data    = response.json()
    hourly  = data.get("hourly", {})
    times   = hourly.get("time", [])

    rows = []
    for i, ts in enumerate(times):
        dt   = datetime.fromisoformat(ts)
        rows.append({
            "city":             city["name"],
            "latitude":         city["lat"],
            "longitude":        city["lon"],
            "date":             dt.strftime("%Y-%m-%d"),
            "hour":             dt.hour,
            "temperature_c":    _safe_float(hourly.get("temperature_2m",  [None])[i]),
            "precipitation_mm": _safe_float(hourly.get("precipitation",   [None])[i]),
            "windspeed_kmh":    _safe_float(hourly.get("windspeed_10m",   [None])[i]),
            "cloudcover_pct":   _safe_float(hourly.get("cloudcover",      [None])[i]),
            "weathercode":      _safe_int(hourly.get("weathercode",       [None])[i]),
        })

    logger.info(f"  → {len(rows)} hourly records fetched for {city['name']}")
    return rows


def load_raw_weather(rows: list[dict], conn: sqlite3.Connection) -> int:
    """
    Insert raw weather rows. Skips duplicates (city + date + hour).
    Returns number of rows inserted.
    """
    if not rows:
        return 0

    cursor = conn.cursor()

    # Avoid re-inserting rows we already have
    cursor.execute(
        "SELECT city, date, hour FROM raw_weather WHERE city = ? AND date >= ?",
        (rows[0]["city"], rows[0]["date"])
    )
    existing = {(r[0], r[1], r[2]) for r in cursor.fetchall()}

    new_rows = [
        r for r in rows
        if (r["city"], r["date"], r["hour"]) not in existing
    ]

    if not new_rows:
        logger.info(f"  → No new rows for {rows[0]['city']} (all already loaded)")
        return 0

    cursor.executemany("""
        INSERT INTO raw_weather
            (city, latitude, longitude, date, hour,
             temperature_c, precipitation_mm, windspeed_kmh,
             cloudcover_pct, weathercode)
        VALUES
            (:city, :latitude, :longitude, :date, :hour,
             :temperature_c, :precipitation_mm, :windspeed_kmh,
             :cloudcover_pct, :weathercode)
    """, new_rows)

    conn.commit()
    logger.info(f"  → Inserted {len(new_rows)} new rows for {new_rows[0]['city']}")
    return len(new_rows)


def run_weather_ingestion(conn: sqlite3.Connection) -> int:
    """
    Main entry point. Runs ingestion for all configured cities.
    Returns total rows inserted.
    """
    total = 0
    for city in CITIES:
        try:
            rows    = fetch_weather(city)
            inserted = load_raw_weather(rows, conn)
            total   += inserted
        except requests.RequestException as e:
            logger.error(f"API error for {city['name']}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for {city['name']}: {e}", exc_info=True)

    logger.info(f"Weather ingestion complete — {total} total rows inserted")
    return total


# ── Helpers ────────────────────────────────────────────────────

def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None
