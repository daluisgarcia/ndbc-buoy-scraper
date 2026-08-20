-- Postgres DDL contract for the bronze -> silver -> Postgres pipeline's load.
--
-- SHARED CONTRACT: `ndbc_buoy_scraper/silver.py::load_postgres` (DuckDB
-- `postgres` extension) inserts POSITIONALLY into the staging tables it
-- creates from these templates. The column order in each CREATE TABLE below
-- MUST match silver.py's OBSERVATION_COLUMNS / COORDINATE_COLUMNS EXACTLY,
-- or the load silently corrupts (values land in the wrong column).
--
-- Mounted by docker-compose.yml at /docker-entrypoint-initdb.d/init.sql, so
-- Postgres runs this automatically on first container init (fresh pgdata
-- volume ONLY -- Postgres skips init scripts on every subsequent start). If
-- you change this file against an existing volume, you must apply the change
-- by hand or recreate the volume with `docker compose down -v`.

-- ---------------------------------------------------------------------------
-- Dimension table, loaded by silver.py::load_coordinates. Small (one row per
-- NDBC station, ~1.4k rows), so it is a plain table -- but it is still loaded
-- via staging + RENAME swap rather than TRUNCATE, so readers never observe an
-- empty dimension mid-load.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS buoy_coordinates (
    buoy_id    TEXT PRIMARY KEY,
    latitude   DOUBLE PRECISION,   -- nullable (fragile XPath scrape, see spider)
    longitude  DOUBLE PRECISION    -- nullable
);

-- ---------------------------------------------------------------------------
-- Fact table, loaded by silver.py::load_observations_year.
--
-- PARTITIONED BY RANGE (datetime), one partition per calendar year. This is
-- the load strategy, not just a query optimization:
--
--   * NDBC's historical year files are immutable once a year closes -- only
--     the in-progress year keeps changing. Yearly partitions let the pipeline
--     rebuild ONLY the years whose bronze actually changed.
--   * Rebuilding a year means loading a detached staging table and swapping
--     it in with DETACH/ATTACH. The swap is a metadata operation measured in
--     milliseconds. The previous TRUNCATE + INSERT approach held an
--     ACCESS EXCLUSIVE lock on the whole table for the entire load, blocking
--     every reader -- at full NDBC scale (1,351 stations / 17,050 year files)
--     that is hours of downtime per run.
--
-- The UNIQUE constraint includes `datetime`, which Postgres requires: a
-- unique constraint on a partitioned table must contain the partition key.
--
-- There is deliberately NO DEFAULT partition. A row whose datetime falls
-- outside every declared year is a data defect, and silver.py filters and
-- COUNTS such rows per year rather than hiding them in a catch-all. A DEFAULT
-- partition would also force Postgres to full-scan it on every ATTACH to
-- prove no overlap, making each swap O(table) instead of O(1).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS buoy_observations (
    buoy_id                  TEXT             NOT NULL,
    datetime                 TIMESTAMP        NOT NULL,
    wind_direction           DOUBLE PRECISION,
    wind_speed               DOUBLE PRECISION,
    gust                     DOUBLE PRECISION,
    significant_wave_height  DOUBLE PRECISION,
    dominant_wave_period     DOUBLE PRECISION,
    average_wave_period      DOUBLE PRECISION,
    mean_wave_direction      DOUBLE PRECISION,
    pressure                 DOUBLE PRECISION,
    air_temperature          DOUBLE PRECISION,
    sea_surface_temperature  DOUBLE PRECISION,
    dew_point_temperature    DOUBLE PRECISION,
    visibility               DOUBLE PRECISION,
    pressure_tendency        DOUBLE PRECISION,
    tide                     DOUBLE PRECISION,
    CONSTRAINT uq_buoy_obs UNIQUE (buoy_id, datetime)
) PARTITION BY RANGE (datetime);

-- NOTE: there is no separate index on (buoy_id). `uq_buoy_obs` is a btree on
-- (buoy_id, datetime), and buoy_id is its leftmost prefix, so that index
-- already serves every buoy_id-only lookup. The former `ix_buoy_obs_buoy` was
-- pure duplicate -- 86 MB of disk and a write amplification on every insert,
-- buying nothing.

-- Year partitions are created on demand by
-- silver.py::ensure_observation_partition -- new NDBC years appear every
-- January and the pipeline must not need a manual DDL step to absorb them.

-- ---------------------------------------------------------------------------
-- Pipeline observability. The job writes its own run record here, which makes
-- Postgres -- the database you already operate and already query -- the
-- monitoring backend. No extra service to run for a portfolio deployment.
--
-- `scrapy_stats` holds Scrapy's own end-of-crawl stats dict verbatim
-- (item_scraped_count, downloader/response_status_count/*, finish_reason,
-- ...), so crawl health is queryable with plain SQL instead of grepping logs.
-- ---------------------------------------------------------------------------
-- `run_token` exists because DuckDB's `postgres_query` wraps every statement
-- in `COPY (...) TO STDOUT`, which rejects DML -- so `INSERT ... RETURNING id`
-- is unavailable and the writer cannot learn its own row id directly. The
-- client generates a UUID, inserts it, then looks the row up by token. That is
-- race-free regardless of concurrent writers, unlike `SELECT max(id)`.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    run_token       UUID        NOT NULL UNIQUE,
    stage           TEXT        NOT NULL,             -- 'scrape' | 'silver'
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL,             -- 'running' | 'ok' | 'failed'
    rows_loaded     BIGINT,
    years_loaded    INT,
    rows_rejected   BIGINT,                           -- datetime outside its year partition
    scrapy_stats    JSONB,
    error           TEXT,
    CONSTRAINT ck_pipeline_runs_status
        CHECK (status IN ('running', 'ok', 'failed'))
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_recent
    ON pipeline_runs (stage, started_at DESC);

-- ---------------------------------------------------------------------------
-- Per-year load ledger. NDBC's closed-year files never change again, so a full
-- reload of every year on every run is almost entirely wasted work: at full
-- scale that is ~48 partitions rebuilt to pick up changes in one of them.
--
-- silver.py fingerprints each year's bronze files (file count + newest mtime)
-- and skips any year whose fingerprint still matches the last successful load.
-- Set SILVER_FORCE=1 to rebuild regardless.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observation_partitions (
    year                INT         PRIMARY KEY,
    bronze_fingerprint  TEXT        NOT NULL,
    rows_loaded         BIGINT      NOT NULL,
    rows_rejected       BIGINT      NOT NULL DEFAULT 0,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
