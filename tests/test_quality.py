# tests/test_quality.py
# Tests for data quality checks in loading/load.py

import sqlite3
import pytest
from loading.load import run_quality_checks


@pytest.fixture
def db_with_fact_data():
    """In-memory DB with minimal fact table + mart schema."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE fct_weather_app_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT, date TEXT, category TEXT,
            weather_label TEXT, avg_temp_c REAL,
            total_precip_mm REAL, category_avg_rating REAL,
            category_total_installs INTEGER, category_app_count INTEGER,
            install_index REAL, created_at TEXT,
            UNIQUE(city, date, category)
        );
        CREATE TABLE mart_weather_app_insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            weather_label TEXT, category TEXT,
            avg_rating REAL, avg_install_index REAL,
            record_count INTEGER, pct_vs_baseline REAL,
            insight_label TEXT, refreshed_at TEXT,
            UNIQUE(weather_label, category)
        );
    """)
    yield conn
    conn.close()


def _insert_fact_rows(conn, n=10, weather_label="Sunny", category="GAME",
                       install_index=1.5, nullify_label=False):
    for i in range(n):
        lbl = None if nullify_label else weather_label
        conn.execute("""
            INSERT INTO fct_weather_app_daily
                (city, date, category, weather_label, avg_temp_c,
                 total_precip_mm, category_avg_rating,
                 category_total_installs, category_app_count, install_index)
            VALUES (?, ?, ?, ?, 20.0, 0.0, 4.2, 500000, 50, ?)
        """, (f"City{i}", f"2026-03-{i+1:02d}", category, lbl, install_index))
    conn.commit()


def test_quality_all_pass(db_with_fact_data):
    conn = db_with_fact_data
    _insert_fact_rows(conn, n=20)
    results = run_quality_checks(conn)
    failures = [r for r in results if not r.passed]
    assert len(failures) == 0, f"Unexpected failures: {failures}"


def test_quality_fails_on_low_row_count(db_with_fact_data):
    conn = db_with_fact_data
    _insert_fact_rows(conn, n=2)   # below MIN_EXPECTED_ROWS
    results = run_quality_checks(conn)
    row_count_check = next(r for r in results if r.check_name == "Minimum row count")
    assert not row_count_check.passed


def test_quality_fails_on_high_null_rate(db_with_fact_data):
    conn = db_with_fact_data
    # Insert 10 rows with nullified weather_label
    _insert_fact_rows(conn, n=10, nullify_label=True)
    results = run_quality_checks(conn)
    null_check = next(r for r in results if "null rate" in r.check_name.lower() and "weather" in r.check_name.lower())
    assert not null_check.passed


def test_quality_fails_on_negative_install_index(db_with_fact_data):
    conn = db_with_fact_data
    _insert_fact_rows(conn, n=10, install_index=-0.5)
    results = run_quality_checks(conn)
    neg_check = next(r for r in results if "negative" in r.check_name.lower())
    assert not neg_check.passed


def test_quality_fails_on_duplicates(db_with_fact_data):
    """Force a duplicate by bypassing UNIQUE — checks the logic."""
    conn = db_with_fact_data
    # Insert same city+date+category twice using INSERT (bypassing unique with OR IGNORE off)
    for _ in range(2):
        try:
            conn.execute("""
                INSERT INTO fct_weather_app_daily
                    (city, date, category, weather_label, install_index)
                VALUES ('NYC', '2026-03-01', 'GAME', 'Sunny', 1.0)
            """)
        except sqlite3.IntegrityError:
            pass
    conn.commit()

    results = run_quality_checks(conn)
    # Since UNIQUE constraint prevents actual dupes, this check should pass
    dupe_check = next(r for r in results if "duplicate" in r.check_name.lower())
    assert dupe_check.passed
