# config/settings.py
# Central config — change cities, thresholds, and paths here

import os

# ── Cities to track ────────────────────────────────────────────
# Open-Meteo uses lat/lon — add any city you want
CITIES = [
    {"name": "New York",     "lat": 40.7128, "lon": -74.0060},
    {"name": "Los Angeles",  "lat": 34.0522, "lon": -118.2437},
    {"name": "Chicago",      "lat": 41.8781, "lon": -87.6298},
    {"name": "Houston",      "lat": 29.7604, "lon": -95.3698},
    {"name": "Philadelphia", "lat": 39.9526, "lon": -75.1652},
]

# ── API Config ─────────────────────────────────────────────────
WEATHER_API_BASE = "https://api.open-meteo.com/v1/forecast"
WEATHER_PARAMS = [
    "temperature_2m",
    "precipitation",
    "weathercode",
    "windspeed_10m",
    "cloudcover",
]
WEATHER_LOOKBACK_DAYS = 7   # how many days back to pull on each run

# ── Database ───────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "pipeline.db")

# ── Data Quality Thresholds ────────────────────────────────────
MAX_NULL_RATE_PCT      = 10.0   # fail if >10% nulls in key columns
MIN_EXPECTED_ROWS      = 5      # fail if fewer rows than this loaded
TEMP_RANGE_MIN_C       = -60    # sanity check on temperature
TEMP_RANGE_MAX_C       = 60
PRECIP_MAX_MM          = 500    # max plausible daily precipitation

# ── Logging ───────────────────────────────────────────────────
LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

# ── Scheduler ─────────────────────────────────────────────────
SCHEDULE_TIME = "06:00"   # run pipeline daily at 6am local time

# ── App categories to track ───────────────────────────────────
# These are the categories we focus on for weather correlation
TRACKED_CATEGORIES = [
    "GAME",
    "VIDEO_PLAYERS",
    "HEALTH_AND_FITNESS",
    "SPORTS",
    "PRODUCTIVITY",
    "SOCIAL",
    "WEATHER",
    "TRAVEL_AND_LOCAL",
]
