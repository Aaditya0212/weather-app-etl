# pipeline.py
# Main ETL orchestrator — runs all stages in order
# Usage: python pipeline.py

import logging
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

from config.settings import DB_PATH, LOG_FORMAT, LOG_LEVEL
from ingestion.weather_ingest import run_weather_ingestion
from ingestion.app_ingest import load_app_data
from transformation.transform import transform_weather, transform_apps, build_fact_table
from loading.load import run_loading

# ── Logging setup ──────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", mode="a"),
    ]
)
logger = logging.getLogger(__name__)


def init_db(conn: sqlite3.Connection) -> None:
    """Create schema if tables don't exist yet."""
    schema_path = Path("sql/create_schema.sql")
    with open(schema_path) as f:
        conn.executescript(f.read())
    logger.info("Database schema initialised")


def log_stage(conn: sqlite3.Connection, run_id: str, stage: str,
              status: str, rows: int = 0, message: str = "",
              started_at: str = "") -> None:
    """Write a pipeline audit log entry."""
    conn.execute("""
        INSERT INTO pipeline_log
            (run_id, stage, status, rows_processed, message, started_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (run_id, stage, status, rows, message, started_at))
    conn.commit()


def run_pipeline() -> bool:
    """
    Runs the full ETL pipeline:
      1. Init DB
      2. Ingest weather (API)
      3. Ingest app data (CSV seed)
      4. Transform weather → stg_weather
      5. Transform apps → stg_apps
      6. Build fact table (join)
      7. Quality checks + mart refresh

    Returns True on success, False on failure.
    """
    run_id     = str(uuid.uuid4())[:8]
    started    = datetime.utcnow().isoformat()
    logger.info(f"{'='*55}")
    logger.info(f"  Pipeline run started | run_id={run_id} | {started}")
    logger.info(f"{'='*55}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")   # better concurrency

    try:
        # Stage 0: Init schema
        init_db(conn)

        # Stage 1: Ingest weather
        t0 = datetime.utcnow().isoformat()
        logger.info("[ 1/5 ] Ingesting weather data...")
        try:
            weather_rows = run_weather_ingestion(conn)
            log_stage(conn, run_id, "ingest_weather", "success", weather_rows, started_at=t0)
        except Exception as e:
            log_stage(conn, run_id, "ingest_weather", "failed", message=str(e), started_at=t0)
            logger.error(f"Weather ingestion failed: {e}", exc_info=True)
            return False

        # Stage 2: Seed app data
        t0 = datetime.utcnow().isoformat()
        logger.info("[ 2/5 ] Seeding app data...")
        try:
            app_rows = load_app_data(conn)
            log_stage(conn, run_id, "ingest_apps", "success", app_rows, started_at=t0)
        except Exception as e:
            log_stage(conn, run_id, "ingest_apps", "failed", message=str(e), started_at=t0)
            logger.warning(f"App ingestion warning (non-fatal): {e}")
            # Non-fatal — continue with existing app data if already seeded

        # Stage 3: Transform weather
        t0 = datetime.utcnow().isoformat()
        logger.info("[ 3/5 ] Transforming weather data...")
        try:
            w_rows = transform_weather(conn)
            log_stage(conn, run_id, "transform_weather", "success", w_rows, started_at=t0)
        except Exception as e:
            log_stage(conn, run_id, "transform_weather", "failed", message=str(e), started_at=t0)
            logger.error(f"Weather transform failed: {e}", exc_info=True)
            return False

        # Stage 4: Transform apps
        t0 = datetime.utcnow().isoformat()
        logger.info("[ 4/5 ] Transforming app data...")
        try:
            a_rows = transform_apps(conn)
            log_stage(conn, run_id, "transform_apps", "success", a_rows, started_at=t0)
        except Exception as e:
            log_stage(conn, run_id, "transform_apps", "failed", message=str(e), started_at=t0)
            logger.error(f"App transform failed: {e}", exc_info=True)
            return False

        # Stage 5: Build fact table + load mart
        t0 = datetime.utcnow().isoformat()
        logger.info("[ 5/5 ] Building fact table and loading mart...")
        try:
            build_fact_table(conn)
            success = run_loading(conn)
            status  = "success" if success else "failed"
            log_stage(conn, run_id, "load", status, started_at=t0)
            if not success:
                return False
        except Exception as e:
            log_stage(conn, run_id, "load", "failed", message=str(e), started_at=t0)
            logger.error(f"Loading failed: {e}", exc_info=True)
            return False

        logger.info(f"Pipeline completed successfully | run_id={run_id}")
        return True

    finally:
        conn.close()


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
