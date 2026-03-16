-- sql/create_schema.sql
-- Full schema for the weather × app engagement pipeline
-- Run once on first setup — pipeline.py handles this automatically

-- ── RAW LAYER (append-only, never modified after insert) ───────

CREATE TABLE IF NOT EXISTS raw_weather (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    city            TEXT        NOT NULL,
    latitude        REAL        NOT NULL,
    longitude       REAL        NOT NULL,
    date            TEXT        NOT NULL,   -- YYYY-MM-DD
    hour            INTEGER,                -- 0–23
    temperature_c   REAL,
    precipitation_mm REAL,
    windspeed_kmh   REAL,
    cloudcover_pct  REAL,
    weathercode     INTEGER,                -- WMO weather code
    ingested_at     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS raw_apps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    app             TEXT,
    category        TEXT,
    rating          REAL,
    reviews         INTEGER,
    installs        INTEGER,
    type            TEXT,
    price           REAL,
    content_rating  TEXT,
    genres          TEXT,
    ingested_at     TEXT DEFAULT (datetime('now'))
);

-- ── STAGING LAYER (cleaned + typed) ───────────────────────────

CREATE TABLE IF NOT EXISTS stg_weather (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    city            TEXT        NOT NULL,
    date            TEXT        NOT NULL,
    avg_temp_c      REAL,
    total_precip_mm REAL,
    avg_windspeed   REAL,
    avg_cloudcover  REAL,
    dominant_code   INTEGER,
    weather_label   TEXT,       -- Sunny / Rainy / Cloudy / Snowy / Stormy
    is_rainy        INTEGER,    -- boolean 0/1
    is_sunny        INTEGER,
    transformed_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(city, date)
);

CREATE TABLE IF NOT EXISTS stg_apps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    app             TEXT        NOT NULL,
    category        TEXT,
    rating          REAL,
    reviews         INTEGER,
    installs        INTEGER,
    is_free         INTEGER,    -- boolean 0/1
    price           REAL,
    transformed_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(app)
);

-- ── FACT TABLE (joined weather + app category aggregates) ──────

CREATE TABLE IF NOT EXISTS fct_weather_app_daily (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    city            TEXT        NOT NULL,
    date            TEXT        NOT NULL,
    category        TEXT        NOT NULL,
    weather_label   TEXT,
    avg_temp_c      REAL,
    total_precip_mm REAL,
    category_avg_rating     REAL,
    category_total_installs INTEGER,
    category_app_count      INTEGER,
    install_index   REAL,       -- installs relative to category baseline
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(city, date, category)
);

-- ── ANALYTICS MART (final output for dashboards) ──────────────

CREATE TABLE IF NOT EXISTS mart_weather_app_insights (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    weather_label       TEXT        NOT NULL,
    category            TEXT        NOT NULL,
    avg_rating          REAL,
    avg_install_index   REAL,
    record_count        INTEGER,
    pct_vs_baseline     REAL,       -- % above/below overall average
    insight_label       TEXT,       -- human readable: "Rainy days → +18% Gaming"
    refreshed_at        TEXT DEFAULT (datetime('now')),
    UNIQUE(weather_label, category)
);

-- ── PIPELINE AUDIT LOG ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT        NOT NULL,
    stage           TEXT        NOT NULL,   -- ingest / transform / load / quality
    status          TEXT        NOT NULL,   -- success / failed / skipped
    rows_processed  INTEGER,
    message         TEXT,
    started_at      TEXT,
    completed_at    TEXT DEFAULT (datetime('now'))
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_raw_weather_city_date   ON raw_weather(city, date);
CREATE INDEX IF NOT EXISTS idx_stg_weather_label       ON stg_weather(weather_label);
CREATE INDEX IF NOT EXISTS idx_fct_city_date           ON fct_weather_app_daily(city, date);
CREATE INDEX IF NOT EXISTS idx_fct_category            ON fct_weather_app_daily(category);
