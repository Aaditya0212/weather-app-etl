# ingestion/app_ingest.py
# Seeds app data from Google Play Store CSV into raw_apps table
# Run once on setup — subsequent runs skip already-loaded apps

import csv
import logging
import re
import sqlite3

logger = logging.getLogger(__name__)

CSV_PATH = "data/googleplaystore.csv"


def parse_installs(val: str) -> int | None:
    """Strip '+', commas, cast to int."""
    try:
        return int(re.sub(r"[^0-9]", "", str(val)))
    except (ValueError, TypeError):
        return None


def parse_price(val: str) -> float:
    """Strip '$', cast to float."""
    try:
        return float(str(val).replace("$", "").strip())
    except (ValueError, TypeError):
        return 0.0


def parse_rating(val: str) -> float | None:
    try:
        r = float(val)
        return r if 1.0 <= r <= 5.0 else None
    except (ValueError, TypeError):
        return None


def parse_reviews(val: str) -> int | None:
    try:
        return int(re.sub(r"[^0-9]", "", str(val)))
    except (ValueError, TypeError):
        return None


def load_app_data(conn: sqlite3.Connection, csv_path: str = CSV_PATH) -> int:
    """
    Seeds raw_apps from CSV. Skips rows already in the table.
    Returns number of rows inserted.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw_apps")
    existing_count = cursor.fetchone()[0]

    if existing_count > 0:
        logger.info(f"raw_apps already has {existing_count} rows — skipping seed")
        return 0

    rows = []
    seen_apps = set()

    try:
        with open(csv_path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                app_name = row.get("App", "").strip()
                if not app_name or app_name in seen_apps:
                    continue
                seen_apps.add(app_name)

                rows.append({
                    "app":            app_name,
                    "category":       row.get("Category", "").strip().upper(),
                    "rating":         parse_rating(row.get("Rating", "")),
                    "reviews":        parse_reviews(row.get("Reviews", "")),
                    "installs":       parse_installs(row.get("Installs", "")),
                    "type":           row.get("Type", "").strip(),
                    "price":          parse_price(row.get("Price", "0")),
                    "content_rating": row.get("Content Rating", "").strip(),
                    "genres":         row.get("Genres", "").strip(),
                })
    except FileNotFoundError:
        logger.warning(
            f"CSV not found at {csv_path}. "
            "Download from: https://www.kaggle.com/datasets/lava18/google-play-store-apps"
        )
        return 0

    cursor.executemany("""
        INSERT INTO raw_apps
            (app, category, rating, reviews, installs,
             type, price, content_rating, genres)
        VALUES
            (:app, :category, :rating, :reviews, :installs,
             :type, :price, :content_rating, :genres)
    """, rows)

    conn.commit()
    logger.info(f"Seeded {len(rows)} app records into raw_apps")
    return len(rows)
