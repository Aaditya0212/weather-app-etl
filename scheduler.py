# scheduler.py
# Runs the pipeline on a daily schedule
# Usage: python scheduler.py
# Keeps running in the background — use screen/tmux or Docker to persist

import logging
import sys

import schedule
import time

from config.settings import LOG_FORMAT, LOG_LEVEL, SCHEDULE_TIME
from pipeline import run_pipeline

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=LOG_FORMAT,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def job():
    logger.info("Scheduled pipeline run starting...")
    success = run_pipeline()
    if not success:
        logger.error("Scheduled pipeline run FAILED — check pipeline.log")


# Run immediately on start, then daily at configured time
logger.info(f"Scheduler started — pipeline runs daily at {SCHEDULE_TIME}")
logger.info("Running initial pipeline now...")
job()

schedule.every().day.at(SCHEDULE_TIME).do(job)

while True:
    schedule.run_pending()
    time.sleep(60)
