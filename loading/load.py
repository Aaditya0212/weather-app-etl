# loading/load.py
# Final loading stage — runs quality checks then writes to analytics mart
# Incremental: only refreshes mart for weather/category combos with new data

import logging
import sqlite3
from dataclasses import dataclass

from config.settings import MAX_NULL_RATE_PCT, MIN_EXPECTED_ROWS

logger = logging.getLogger(__name__)


@dataclass
class QualityResult:
    check_name: str
    passed: bool
    detail: str


def run_quality_checks(conn: sqlite3.Connection) -> list[QualityResult]:
    """
    Runs data quality checks on the fact table before mart refresh.
    Returns list of QualityResult — pipeline should halt if any fail.
    """
    cursor = conn.cursor()
    results = []

    # CHECK 1: Minimum row count
    cursor.execute("SELECT COUNT(*) FROM fct_weather_app_daily")
    row_count = cursor.fetchone()[0]
    results.append(QualityResult(
        check_name="Minimum row count",
        passed=row_count >= MIN_EXPECTED_ROWS,
        detail=f"{row_count} rows (min: {MIN_EXPECTED_ROWS})"
    ))

    # CHECK 2: Null rate on weather_label
    cursor.execute("""
        SELECT
            100.0 * COUNT(*) FILTER (WHERE weather_label IS NULL) / COUNT(*)
        FROM fct_weather_app_daily
    """)
    null_rate = cursor.fetchone()[0] or 0
    results.append(QualityResult(
        check_name="weather_label null rate",
        passed=null_rate <= MAX_NULL_RATE_PCT,
        detail=f"{null_rate:.1f}% null (max: {MAX_NULL_RATE_PCT}%)"
    ))

    # CHECK 3: Null rate on category
    cursor.execute("""
        SELECT
            100.0 * COUNT(*) FILTER (WHERE category IS NULL) / COUNT(*)
        FROM fct_weather_app_daily
    """)
    null_rate_cat = cursor.fetchone()[0] or 0
    results.append(QualityResult(
        check_name="category null rate",
        passed=null_rate_cat <= MAX_NULL_RATE_PCT,
        detail=f"{null_rate_cat:.1f}% null (max: {MAX_NULL_RATE_PCT}%)"
    ))

    # CHECK 4: No duplicate city+date+category combos
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT city, date, category, COUNT(*) AS cnt
            FROM fct_weather_app_daily
            GROUP BY city, date, category
            HAVING cnt > 1
        )
    """)
    dupes = cursor.fetchone()[0]
    results.append(QualityResult(
        check_name="No duplicate city+date+category",
        passed=dupes == 0,
        detail=f"{dupes} duplicate groups found"
    ))

    # CHECK 5: install_index sanity (no negatives)
    cursor.execute("""
        SELECT COUNT(*) FROM fct_weather_app_daily
        WHERE install_index IS NOT NULL AND install_index < 0
    """)
    neg_index = cursor.fetchone()[0]
    results.append(QualityResult(
        check_name="No negative install_index",
        passed=neg_index == 0,
        detail=f"{neg_index} negative values found"
    ))

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        logger.info(f"  QC [{status}] {r.check_name}: {r.detail}")

    return results


def build_mart(conn: sqlite3.Connection) -> int:
    """
    Aggregates fact table into final analytics mart.
    Computes pct_vs_baseline and insight labels.
    Uses INSERT OR REPLACE for idempotent refreshes.
    """
    cursor = conn.cursor()

    # Overall baseline: average install_index across all weather types
    cursor.execute("""
        SELECT AVG(install_index) FROM fct_weather_app_daily
        WHERE install_index IS NOT NULL
    """)
    baseline = cursor.fetchone()[0] or 1

    cursor.execute("""
        INSERT OR REPLACE INTO mart_weather_app_insights
            (weather_label, category, avg_rating, avg_install_index,
             record_count, pct_vs_baseline, insight_label, refreshed_at)
        SELECT
            weather_label,
            category,
            ROUND(AVG(category_avg_rating), 2)      AS avg_rating,
            ROUND(AVG(install_index), 4)             AS avg_install_index,
            COUNT(*)                                 AS record_count,
            ROUND(
                100.0 * (AVG(install_index) - ?) / ?, 1
            )                                        AS pct_vs_baseline,
            weather_label || ' days → ' ||
            CASE
                WHEN AVG(install_index) > ? * 1.1  THEN '+' ||
                    CAST(ROUND(100.0 * (AVG(install_index) - ?) / ?, 0) AS TEXT)
                    || '% ' || category
                WHEN AVG(install_index) < ? * 0.9  THEN
                    CAST(ROUND(100.0 * (AVG(install_index) - ?) / ?, 0) AS TEXT)
                    || '% ' || category
                ELSE 'baseline ' || category
            END                                      AS insight_label,
            datetime('now')
        FROM fct_weather_app_daily
        WHERE weather_label IS NOT NULL
          AND category IS NOT NULL
          AND install_index IS NOT NULL
        GROUP BY weather_label, category
        HAVING COUNT(*) >= 3
    """, (baseline, baseline,
          baseline, baseline, baseline,
          baseline, baseline, baseline))

    conn.commit()
    count = cursor.rowcount
    logger.info(f"build_mart: {count} insight rows written to mart_weather_app_insights")
    return count


def print_insights(conn: sqlite3.Connection) -> None:
    """Prints a summary table of top insights to stdout."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT weather_label, category, avg_rating,
               avg_install_index, pct_vs_baseline, insight_label
        FROM mart_weather_app_insights
        ORDER BY ABS(pct_vs_baseline) DESC
        LIMIT 15
    """)
    rows = cursor.fetchall()

    print("\n" + "=" * 65)
    print("  Weather × App Engagement Insights")
    print("=" * 65)
    print(f"  {'Weather':<12} {'Category':<25} {'Rating':<8} {'vs Baseline'}")
    print("-" * 65)
    for r in rows:
        weather, cat, rating, _, pct, label = r
        pct_str = f"+{pct:.1f}%" if pct and pct > 0 else f"{pct:.1f}%"
        print(f"  {weather:<12} {cat:<25} {rating or 'N/A':<8} {pct_str}")
    print("=" * 65 + "\n")


def run_loading(conn: sqlite3.Connection) -> bool:
    """
    Main loading entry point.
    Returns True if all quality checks passed and mart was refreshed.
    """
    logger.info("Running data quality checks...")
    checks = run_quality_checks(conn)
    failures = [c for c in checks if not c.passed]

    if failures:
        for f in failures:
            logger.error(f"Quality check FAILED: {f.check_name} — {f.detail}")
        logger.error("Halting load due to quality failures")
        return False

    logger.info("All quality checks passed — building analytics mart")
    build_mart(conn)
    print_insights(conn)
    return True
