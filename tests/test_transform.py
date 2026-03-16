# tests/test_transform.py
# Unit tests for transformation logic
# Run: pytest tests/ -v

import sqlite3
import pytest
from transformation.transform import classify_weather, transform_weather, transform_apps


# ── classify_weather tests ─────────────────────────────────────

def test_classify_sunny_by_code():
    result = classify_weather(code=0, precip_mm=0)
    assert result["weather_label"] == "Sunny"
    assert result["is_sunny"] == 1
    assert result["is_rainy"] == 0


def test_classify_rainy_by_code():
    result = classify_weather(code=61, precip_mm=5.0)
    assert result["weather_label"] == "Rainy"
    assert result["is_rainy"] == 1
    assert result["is_sunny"] == 0


def test_classify_stormy_by_code():
    result = classify_weather(code=95, precip_mm=10.0)
    assert result["weather_label"] == "Stormy"
    assert result["is_rainy"] == 1


def test_classify_snowy_by_code():
    result = classify_weather(code=71, precip_mm=0)
    assert result["weather_label"] == "Snowy"
    assert result["is_rainy"] == 0
    assert result["is_sunny"] == 0


def test_classify_fallback_to_precip_rainy():
    """When code is None but precip is high, should classify as Rainy."""
    result = classify_weather(code=None, precip_mm=5.0)
    assert result["weather_label"] == "Rainy"


def test_classify_fallback_to_precip_sunny():
    """When code is None and precip is 0, should classify as Sunny."""
    result = classify_weather(code=None, precip_mm=0)
    assert result["weather_label"] == "Sunny"


def test_classify_unknown_code():
    """Out-of-range code with no precip falls back gracefully."""
    result = classify_weather(code=999, precip_mm=None)
    assert result["weather_label"] in ("Unknown", "Sunny", "Cloudy", "Rainy")


# ── transform_weather tests ────────────────────────────────────

@pytest.fixture
def in_memory_db():
    """Provide a fresh in-memory SQLite DB with schema for each test."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=WAL")

    # Minimal schema
    conn.executescript("""
        CREATE TABLE raw_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT, latitude REAL, longitude REAL,
            date TEXT, hour INTEGER,
            temperature_c REAL, precipitation_mm REAL,
            windspeed_kmh REAL, cloudcover_pct REAL,
            weathercode INTEGER, ingested_at TEXT
        );
        CREATE TABLE stg_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT, date TEXT,
            avg_temp_c REAL, total_precip_mm REAL,
            avg_windspeed REAL, avg_cloudcover REAL,
            dominant_code INTEGER, weather_label TEXT,
            is_rainy INTEGER, is_sunny INTEGER,
            transformed_at TEXT,
            UNIQUE(city, date)
        );
        CREATE TABLE stg_apps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app TEXT, category TEXT, rating REAL,
            reviews INTEGER, installs INTEGER,
            is_free INTEGER, price REAL, transformed_at TEXT,
            UNIQUE(app)
        );
        CREATE TABLE raw_apps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app TEXT, category TEXT, rating REAL,
            reviews INTEGER, installs INTEGER,
            type TEXT, price REAL, content_rating TEXT, genres TEXT,
            ingested_at TEXT
        );
    """)
    yield conn
    conn.close()


def test_transform_weather_inserts_rows(in_memory_db):
    """transform_weather should produce one stg row per city+date."""
    conn = in_memory_db
    # Insert 24 hourly rows for one city+date
    for hour in range(24):
        conn.execute("""
            INSERT INTO raw_weather
                (city, latitude, longitude, date, hour,
                 temperature_c, precipitation_mm, windspeed_kmh,
                 cloudcover_pct, weathercode)
            VALUES ('New York', 40.71, -74.00, '2026-03-01', ?,
                    20.0, 0.0, 15.0, 10.0, 0)
        """, (hour,))
    conn.commit()

    rows_written = transform_weather(conn)

    assert rows_written == 1
    cursor = conn.execute("SELECT weather_label, is_sunny FROM stg_weather")
    row = cursor.fetchone()
    assert row[0] == "Sunny"
    assert row[1] == 1


def test_transform_weather_is_incremental(in_memory_db):
    """Running transform twice should not double-insert."""
    conn = in_memory_db
    conn.execute("""
        INSERT INTO raw_weather
            (city, latitude, longitude, date, hour,
             temperature_c, precipitation_mm, windspeed_kmh,
             cloudcover_pct, weathercode)
        VALUES ('Chicago', 41.87, -87.62, '2026-03-01', 12,
                15.0, 3.0, 20.0, 80.0, 61)
    """)
    conn.commit()

    transform_weather(conn)
    second_run = transform_weather(conn)

    assert second_run == 0  # nothing new to process

    cursor = conn.execute("SELECT COUNT(*) FROM stg_weather")
    assert cursor.fetchone()[0] == 1


def test_transform_apps_deduplicates(in_memory_db):
    """transform_apps should deduplicate by app name."""
    conn = in_memory_db
    for i in range(3):
        conn.execute("""
            INSERT INTO raw_apps (app, category, rating, reviews, installs, type, price)
            VALUES ('TestApp', 'GAME', 4.5, 1000, 50000, 'Free', 0.0)
        """)
    conn.commit()

    count = transform_apps(conn)
    assert count == 1  # deduplicated to one row
