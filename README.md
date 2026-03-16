# Weather × App Engagement ETL Pipeline

A production-style data engineering project that ingests live weather data and Google Play Store app metrics, joins them in a daily pipeline, and surfaces insights on how weather patterns influence app category engagement.

> **Business question:** Do people use Gaming, Streaming, and Fitness apps differently on rainy vs sunny days? This pipeline answers that with repeatable, scheduled data.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│                                                                 │
│  ┌──────────────────┐          ┌──────────────────────────────┐ │
│  │  Open-Meteo API  │          │  Google Play Store CSV       │ │
│  │  (Free, no key)  │          │  (Kaggle / static seed)      │ │
│  │  weather by city │          │  app ratings + installs      │ │
│  └────────┬─────────┘          └──────────────┬───────────────┘ │
└───────────┼─────────────────────────────────── ┼───────────────┘
            │                                    │
            ▼                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                            │
│                                                                 │
│   ingestion/weather_ingest.py        ingestion/app_ingest.py   │
│   • Pulls hourly weather per city    • Loads + seeds app data  │
│   • Validates schema on arrival      • Deduplicates on load     │
│   • Writes to raw_weather table      • Writes to raw_apps table │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                   TRANSFORMATION LAYER                          │
│                                                                 │
│   transformation/transform.py                                   │
│   • Cleans types, normalises fields                             │
│   • Classifies weather (Sunny/Rainy/Cloudy/Snowy)              │
│   • Aggregates app metrics by category                          │
│   • Joins weather + app category data                           │
│   • Writes to stg_weather, stg_apps, fct_weather_app_daily     │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      LOADING LAYER                              │
│                                                                 │
│   loading/load.py                                               │
│   • Incremental load — only inserts new dates                   │
│   • Runs data quality checks before committing                  │
│   • Logs row counts, null rates, and load status               │
│   • Writes final output to mart_weather_app_insights           │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA WAREHOUSE (SQLite)                      │
│                                                                 │
│   pipeline.db                                                   │
│   ├── raw_weather          (append-only raw ingest)             │
│   ├── raw_apps             (seeded once from CSV)               │
│   ├── stg_weather          (cleaned + classified)               │
│   ├── stg_apps             (cleaned + categorised)              │
│   ├── fct_weather_app_daily (joined fact table)                 │
│   └── mart_weather_app_insights (final analytics mart)         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Features

| Feature | Detail |
|---|---|
| Live API ingestion | Open-Meteo weather API — free, no key required |
| Incremental loading | Only processes new dates — idempotent reruns |
| Data quality gates | Null checks, row count validation, range checks before commit |
| Modular design | Ingestion / Transformation / Loading are fully independent layers |
| Configurable cities | Add any city in `config/settings.py` |
| Unit tested | `pytest` suite covers transformations and quality checks |
| Dockerised | Runs anywhere with one command |
| Logging | Structured logs for every pipeline stage |

---

## Project Structure

```
weather-app-etl/
│
├── ingestion/
│   ├── weather_ingest.py       # Pulls weather from Open-Meteo API
│   └── app_ingest.py           # Seeds app data from CSV
│
├── transformation/
│   └── transform.py            # Cleans, classifies, joins data
│
├── loading/
│   └── load.py                 # Incremental load + quality checks
│
├── sql/
│   ├── create_schema.sql       # All table definitions
│   └── analytical_queries.sql  # Business insight queries
│
├── tests/
│   ├── test_transform.py       # Unit tests for transform logic
│   └── test_quality.py         # Data quality check tests
│
├── config/
│   └── settings.py             # Cities, API config, thresholds
│
├── pipeline.py                 # Main orchestrator — runs full ETL
├── scheduler.py                # Runs pipeline daily at 6am
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Quickstart

**Option 1 — Docker (recommended)**
```bash
git clone https://github.com/Aaditya0212/weather-app-etl.git
cd weather-app-etl
docker-compose up
```

**Option 2 — Local Python**
```bash
git clone https://github.com/Aaditya0212/weather-app-etl.git
cd weather-app-etl
pip install -r requirements.txt
python pipeline.py
```

**Run tests:**
```bash
pytest tests/ -v
```

---

## Sample Insight Output

```
Weather × App Category Engagement (Last 30 Days)
─────────────────────────────────────────────────
Weather Type   Category      Avg Rating   Install Index
──────────────────────────────────────────────────────
Rainy          GAME          4.21         +18% vs baseline
Rainy          VIDEO_PLAYERS 4.35         +24% vs baseline
Sunny          HEALTH        4.41         +31% vs baseline
Sunny          SPORTS        4.38         +27% vs baseline
Cloudy         PRODUCTIVITY  4.18         +9% vs baseline
```

---

## Tech Stack

`Python 3.11` · `SQLite` · `Open-Meteo API` · `Pandas` · `Requests` · `pytest` · `Docker` · `schedule`

---

## Why This Project

Most ETL portfolios load a CSV and call it a pipeline. This project demonstrates what a real pipeline looks like:

- **Live data** from an actual API, pulled on a schedule
- **Incremental loading** — reruns don't create duplicates
- **Quality gates** — bad data is caught before it reaches the mart
- **Separation of concerns** — each layer does one job
- **Reproducibility** — Docker means it runs the same everywhere

---

**Author:** Aaditya Patel | [LinkedIn](https://linkedin.com/in/aaditya0212) | [GitHub](https://github.com/Aaditya0212)
