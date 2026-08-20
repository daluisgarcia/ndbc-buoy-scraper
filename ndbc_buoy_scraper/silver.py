"""Silver transform: reads mechanically-decoded bronze Parquet, applies ALL
semantic NDBC transforms, and loads two Postgres tables via DuckDB's
`postgres` extension.

This started as the DuckDB port of a since-removed PySpark implementation
(see git history for the original transform and the full sourcing notes on
the sentinel table). Runs on the HOST inside the scraper's Python 3.13 venv
(`uv run python -m ndbc_buoy_scraper.silver`) -- no Docker required for this
path. This module is the ONLY bronze->silver ETL and the pipeline's final
stage: bronze Parquet in, Postgres detail tables out.

Boundary rule: the scraper owns network I/O and mechanical structural
decode; THIS module owns all business/semantic transforms (units-row drop,
rename, year normalize, datetime construction, sentinel nullification, typed
cast, dedup).

LOAD STRATEGY -- read this before changing anything below `configure_duckdb`.

The fact table is loaded ONE YEAR AT A TIME into a detached staging table
that is then swapped in as a partition. It is emphatically NOT a
TRUNCATE + INSERT of the whole table, which is what this module used to do.
Three properties come out of that, and all three matter at NDBC's real scale
(1,351 stations / 17,050 year files, hundreds of millions of rows):

  * NO READER DOWNTIME. `TRUNCATE` takes an ACCESS EXCLUSIVE lock held for the
    entire reload. The DETACH/ATTACH swap is a catalog operation: measured at
    0.8 ms for a 1M-row partition, and constant in partition size PROVIDED the
    staging table carries a CHECK constraint matching its range bound. Without
    that CHECK, Postgres full-scans the partition to prove the bound holds --
    33 ms for the same 1M rows, and growing linearly. `_swap_partition_sql`
    always emits the CHECK first; do not "simplify" it away.
  * NO WASTED REBUILDS. Closed NDBC years are immutable, so each year's bronze
    is fingerprinted (file count + newest mtime) and skipped when unchanged.
  * BOUNDED MEMORY. Only one year is in flight at a time, under an explicit
    DuckDB `memory_limit` -- see `configure_duckdb`.
"""

from __future__ import annotations

import glob as globlib
import os
import re
import sys
import traceback

import duckdb

from ndbc_buoy_scraper import pg

# ---------------------------------------------------------------------------
# DuckDB resource caps. The deployment target is a 4 vCPU / 6 GB box that runs
# Postgres ON THE SAME HOST, and DuckDB's default `memory_limit` is ~80% of
# system RAM -- which on that box means DuckDB alone claims ~4.8 GB and starves
# the database it is loading into. These caps are not tuning, they are a
# correctness requirement for co-resident operation. Spilling to
# `temp_directory` is the intended pressure valve; the target has 600 GB.
# ---------------------------------------------------------------------------
DEFAULT_MEMORY_LIMIT = "1500MB"
DEFAULT_THREADS = 3
DEFAULT_TEMP_DIR = "data/tmp"

# ---------------------------------------------------------------------------
# Canonical column order -- SHARED CONTRACT with db/init.sql AND with
# spark_jobs/transform.py::OBSERVATION_COLUMNS / COORDINATE_COLUMNS. These
# three lists MUST stay in lockstep: the Postgres INSERT below is positional
# (DuckDB's `postgres` extension, like Spark's JDBC writer, inserts by
# column position of the SELECT, not by name), so a drift here silently
# corrupts the load. There is no surrogate `id` column on either table (see
# design section 4, ADR: natural key is (buoy_id, datetime)).
# ---------------------------------------------------------------------------
OBSERVATION_COLUMNS: list[str] = [
    "buoy_id",
    "datetime",
    "wind_direction",
    "wind_speed",
    "gust",
    "significant_wave_height",
    "dominant_wave_period",
    "average_wave_period",
    "mean_wave_direction",
    "pressure",
    "air_temperature",
    "sea_surface_temperature",
    "dew_point_temperature",
    "visibility",
    "pressure_tendency",
    "tide",
]

COORDINATE_COLUMNS: list[str] = [
    "buoy_id",
    "latitude",
    "longitude",
]

# ---------------------------------------------------------------------------
# NDBC raw column -> semantic name rename map (measurement columns only).
# Copied verbatim from spark_jobs/transform.py::MEASUREMENT_RENAME_MAP -- do
# NOT let these two lists drift apart.
# ---------------------------------------------------------------------------
MEASUREMENT_RENAME_MAP: dict[str, str] = {
    "WDIR": "wind_direction",
    "WSPD": "wind_speed",
    "GST": "gust",
    "WVHT": "significant_wave_height",
    "DPD": "dominant_wave_period",
    "APD": "average_wave_period",
    "MWD": "mean_wave_direction",
    "PRES": "pressure",
    "ATMP": "air_temperature",
    "WTMP": "sea_surface_temperature",
    "DEWP": "dew_point_temperature",
    "VIS": "visibility",
    "PTDY": "pressure_tendency",
    "TIDE": "tide",
}

# Raw NDBC year-column header variants seen across historical stdmet files
# (older files use a bare 2-digit `YY`, mid-2000s files use 4-digit `YYYY`,
# current files use `#YY`). Copied verbatim from
# spark_jobs/transform.py::YEAR_COLUMN_CANDIDATES.
YEAR_COLUMN_CANDIDATES: tuple[str, ...] = ("#YY", "YYYY", "YY")

# ---------------------------------------------------------------------------
# Per-column sentinel ("missing value") table -- copied verbatim from
# spark_jobs/transform.py::SENTINEL_VALUES. See that module for the full
# sourcing note (NDBC Measurement Descriptions FAQ + a real historical
# stdmet file for station 41001, year 2020). NEVER a blanket 99.0/999.0
# replace across all columns -- this MUST stay per-column.
# ---------------------------------------------------------------------------
SENTINEL_VALUES: dict[str, tuple[float, ...]] = {
    "wind_direction": (999.0,),  # WDIR
    "wind_speed": (99.0,),  # WSPD
    "gust": (99.0,),  # GST
    "significant_wave_height": (99.0,),  # WVHT (raw "99.00")
    "dominant_wave_period": (99.0,),  # DPD (raw "99.00")
    "average_wave_period": (99.0,),  # APD (raw "99.00")
    "mean_wave_direction": (999.0,),  # MWD
    "pressure": (9999.0,),  # PRES
    "air_temperature": (999.0,),  # ATMP
    "sea_surface_temperature": (999.0,),  # WTMP
    "dew_point_temperature": (999.0,),  # DEWP
    "visibility": (99.0,),  # VIS
    "pressure_tendency": (99.0,),  # PTDY (not present in historical files)
    "tide": (99.0,),  # TIDE (raw "99.00")
}

_MINUTE_RENAME_PATTERN = re.compile(r"^mm_\d+$")


def _quote(identifier: str) -> str:
    """Double-quote a SQL identifier, escaping embedded quotes."""
    return '"' + identifier.replace('"', '""') + '"'


def _coalesce_sql(quoted_exprs: list[str]) -> str:
    if not quoted_exprs:
        return "NULL"
    if len(quoted_exprs) == 1:
        return quoted_exprs[0]
    return f"COALESCE({', '.join(quoted_exprs)})"


def _find_minute_column(columns: list[str]) -> str | None:
    """Resolve the exact physical name of the minute column, working around
    DuckDB's case-insensitive column de-duplication on read.

    CRITICAL GOTCHA: every real NDBC stdmet header carries BOTH `MM` (month)
    and `mm` (minute) -- e.g. "#YY MM DD hh mm WDIR ...". DuckDB's Parquet
    reader folds identifiers case-insensitively for collision detection, and
    when two columns in the same file case-fold to the same name, it does
    NOT raise and does NOT merge them: it silently renames the later
    occurrence to `<name>_1` (verified empirically -- see
    tests/test_silver.py::TestMinuteMonthDistinctness). Since `MM` always
    appears before `mm` in every real stdmet header, `MM` always keeps its
    original name and `mm` is always the one renamed -- to `mm_1` here, but
    we match `mm_<digits>` generically in case DuckDB's exact suffix scheme
    changes across versions. This is the DuckDB-side equivalent of the
    Spark job's `spark.sql.caseSensitive=true` fix (see
    spark_jobs/transform.py::build_spark_session for the original
    reproduction) -- without it, `mm` would resolve to the SAME column as
    `MM` (month), corrupting every datetime built from a file that has a
    minute column.
    """
    for column in columns:
        if column == "MM":
            continue
        lowered = column.lower()
        if lowered == "mm" or _MINUTE_RENAME_PATTERN.match(lowered):
            return column
    return None


def _describe_columns(con: duckdb.DuckDBPyConnection, sql: str) -> list[str]:
    return [row[0] for row in con.execute(f"DESCRIBE {sql}").fetchall()]


def _sql_string(value: str) -> str:
    """Escape a value for embedding in a single-quoted DuckDB string literal."""
    return value.replace("'", "''")


def _read_bronze_sql(glob_pattern: str, hive_types: dict[str, str]) -> str:
    hive_types_sql = ", ".join(f"'{k}': '{v}'" for k, v in hive_types.items())
    return f"""
        SELECT * FROM read_parquet(
            '{_sql_string(glob_pattern)}',
            hive_partitioning = true,
            union_by_name = true,
            hive_types = {{{hive_types_sql}}}
        )
    """


def observations_glob(bronze_root: str, year: str | None = None) -> str:
    """Glob for bronze observations, optionally narrowed to one hive year.

    Narrowing by PATH rather than by a `WHERE year = ...` predicate is
    deliberate. `read_parquet` must open every matched file's footer to unify
    schemas under `union_by_name` before any filter can be applied, so a
    whole-tree glob costs 17,050 footer reads per year loaded. The path glob
    hands DuckDB only that year's files.
    """
    if year is None:
        return os.path.join(bronze_root, "observations", "**", "*.parquet")
    return os.path.join(bronze_root, "observations", "*", f"year={year}", "*.parquet")


def discover_bronze_years(bronze_root: str) -> tuple[list[str], list[str]]:
    """Return (four-digit years present in bronze, unusable year values).

    The spider falls back to `year=unknown` when it cannot parse a year out of
    a source filename. Those partitions cannot be routed to a yearly Postgres
    partition without opening them, and a file whose year is unknown is a
    scrape anomaly worth surfacing rather than silently folding in -- so they
    are returned separately for the caller to report, not quietly dropped.
    """
    pattern = os.path.join(bronze_root, "observations", "*", "year=*")
    years: set[str] = set()
    unusable: set[str] = set()
    for path in globlib.glob(pattern):
        if not os.path.isdir(path):
            continue
        value = os.path.basename(path).split("=", 1)[1]
        if len(value) == 4 and value.isdigit():
            years.add(value)
        else:
            unusable.add(value)
    return sorted(years), sorted(unusable)


def fingerprint_bronze_year(bronze_root: str, year: str) -> str:
    """Cheap change-detector for one year's bronze files: count + newest mtime.

    Deliberately not a content hash. The pipeline re-reads every file it
    considers changed, so a hash would cost a full read of the very data the
    fingerprint exists to avoid reading. The scraper only ever rewrites a
    bronze file wholesale (`to_parquet` on a fresh partition path), so mtime
    moves whenever content does.
    """
    paths = globlib.glob(observations_glob(bronze_root, year))
    if not paths:
        return "empty"
    newest = max(os.path.getmtime(p) for p in paths)
    return f"{len(paths)}:{newest:.6f}"


def configure_duckdb(
    con: duckdb.DuckDBPyConnection,
    *,
    memory_limit: str | None = None,
    threads: int | None = None,
    temp_directory: str | None = None,
) -> None:
    """Apply the co-residency resource caps described in the module docstring."""
    memory_limit = memory_limit or os.environ.get(
        "DUCKDB_MEMORY_LIMIT", DEFAULT_MEMORY_LIMIT
    )
    threads = threads or int(os.environ.get("DUCKDB_THREADS", DEFAULT_THREADS))
    temp_directory = temp_directory or os.environ.get(
        "DUCKDB_TEMP_DIR", DEFAULT_TEMP_DIR
    )

    os.makedirs(temp_directory, exist_ok=True)
    con.execute(f"SET memory_limit = '{_sql_string(memory_limit)}'")
    con.execute(f"SET threads = {int(threads)}")
    con.execute(f"SET temp_directory = '{_sql_string(temp_directory)}'")
    # Row order carries no meaning here (dedup already keeps an arbitrary row
    # per key), and dropping the ordering guarantee lets DuckDB stream the scan
    # instead of buffering it -- the single largest memory win on a bulk load.
    con.execute("SET preserve_insertion_order = false")


def build_observations_sql(
    con: duckdb.DuckDBPyConnection, bronze_root: str, year: str | None = None
) -> str:
    """Build the full observations transform as a single SQL query string
    against bronze Parquet under `<bronze_root>/observations/`.

    Mirrors spark_jobs/transform.py's pipeline step-by-step:
      1. read_bronze   -- hive partitioning (buoy_id/year) + schema-drift
                           tolerant union (union_by_name).
      2. drop_units_rows -- the NDBC units row (e.g. "#yr mo dy hr mn degT
                           ...") was deliberately kept in bronze as an
                           ordinary data row; filtered here by requiring the
                           raw year field to be purely numeric (2-4 digits).
      3. normalize_columns -- coalesce year/minute header variants, rename
                           NDBC measurement codes to semantic names
                           (presence-safe: an absent column becomes NULL
                           instead of raising).
      4. normalize_year -- 2-digit -> 4-digit year (pivot at 50: <50 -> 20xx,
                           >=50 -> 19xx); 4-digit years pass through.
      5. build_datetime -- construct `datetime` WITH minute folded in via
                           make_timestamp (no string parsing, no timezone
                           conversion -- NDBC data is already UTC and
                           DuckDB's TIMESTAMP is timezone-naive, so no
                           session-timezone config is needed here, unlike
                           Spark's `to_timestamp`).
      6. nullify_sentinels -- TRY_CAST each measurement column to DOUBLE
                           (which also nulls the literal string marker "MM"),
                           then null out the column-specific NDBC sentinel
                           value(s) from SENTINEL_VALUES. Per-column, never
                           blanket.
      7. dedup          -- ROW_NUMBER() OVER (PARTITION BY buoy_id, datetime)
                           = 1, keeping an arbitrary row per (buoy_id,
                           datetime) -- same "keep-first is acceptable for
                           v1" contract as the Spark job's dropDuplicates.
      8. final select   -- OBSERVATION_COLUMNS, exact order.
    """
    probe_sql = _read_bronze_sql(
        observations_glob(bronze_root, year),
        {"buoy_id": "VARCHAR", "year": "VARCHAR"},
    )
    columns = _describe_columns(con, probe_sql)
    available = set(columns)

    # Bronze is NOT type-stable: the scraper writes one Parquet per source
    # file via pandas, which infers dtypes per file. Historical NDBC files
    # carry no units row, so pandas types their columns numerically (BIGINT),
    # while post-2006 files -- whose units row makes every column textual --
    # land as VARCHAR. `union_by_name` then surfaces sibling year variants
    # with DIFFERENT types (e.g. `#YY` VARCHAR alongside `YYYY` BIGINT), and
    # COALESCE refuses to mix them. Normalize every structural field to
    # VARCHAR before combining: downstream steps already TRY_CAST from text.
    year_candidates = [
        f"CAST({_quote(c)} AS VARCHAR)" for c in YEAR_COLUMN_CANDIDATES if c in available
    ]
    year_raw_expr = _coalesce_sql(year_candidates)

    minute_column = _find_minute_column(columns)
    minute_raw_expr = (
        f"COALESCE(CAST({_quote(minute_column)} AS VARCHAR), '0')"
        if minute_column
        else "'0'"
    )

    measurement_selects = []
    for raw_name, semantic_name in MEASUREMENT_RENAME_MAP.items():
        if raw_name in available:
            measurement_selects.append(f"{_quote(raw_name)} AS {semantic_name}")
        else:
            measurement_selects.append(f"CAST(NULL AS VARCHAR) AS {semantic_name}")

    # Step 2: drop the units row. The units row holds text (e.g. "#yr") in
    # the raw year field regardless of which year-column variant a given
    # file used, so filtering on that field covers every variant.
    filtered_sql = f"""
        SELECT *
        FROM ({probe_sql}) AS raw
        WHERE regexp_matches(CAST({year_raw_expr} AS VARCHAR), '^[0-9]{{2,4}}$')
    """

    # Step 3: coalesce year/minute variants, rename measurement columns.
    normalized_sql = f"""
        SELECT
            CAST(buoy_id AS VARCHAR) AS buoy_id,
            {year_raw_expr} AS year_raw,
            {minute_raw_expr} AS minute_raw,
            {_quote("MM")} AS month_raw,
            {_quote("DD")} AS day_raw,
            {_quote("hh")} AS hour_raw,
            {", ".join(measurement_selects)}
        FROM ({filtered_sql}) AS filtered
    """

    # Step 4: 2-digit -> 4-digit year, pivot at 50.
    year4_sql = f"""
        SELECT
            *,
            CASE
                WHEN LENGTH(CAST(year_raw AS VARCHAR)) = 2 THEN
                    CASE
                        WHEN TRY_CAST(year_raw AS INTEGER) < 50
                            THEN '20' || CAST(year_raw AS VARCHAR)
                        ELSE '19' || CAST(year_raw AS VARCHAR)
                    END
                ELSE CAST(year_raw AS VARCHAR)
            END AS year4
        FROM ({normalized_sql}) AS normalized
    """

    # Step 5: build the datetime column, minute folded in.
    measurement_cols_sql = ", ".join(MEASUREMENT_RENAME_MAP.values())
    datetime_sql = f"""
        SELECT
            buoy_id,
            make_timestamp(
                TRY_CAST(year4 AS INTEGER),
                TRY_CAST(month_raw AS INTEGER),
                TRY_CAST(day_raw AS INTEGER),
                TRY_CAST(hour_raw AS INTEGER),
                TRY_CAST(minute_raw AS INTEGER),
                0
            ) AS datetime,
            {measurement_cols_sql}
        FROM ({year4_sql}) AS with_year4
    """

    # Step 6: per-column sentinel -> NULL, cast to DOUBLE.
    sentinel_selects = []
    for semantic_name in MEASUREMENT_RENAME_MAP.values():
        sentinels = SENTINEL_VALUES.get(semantic_name, ())
        double_expr = f"TRY_CAST({semantic_name} AS DOUBLE)"
        if sentinels:
            values_sql = ", ".join(repr(float(v)) for v in sentinels)
            sentinel_selects.append(
                f"CASE WHEN {double_expr} IN ({values_sql}) THEN NULL "
                f"ELSE {double_expr} END AS {semantic_name}"
            )
        else:
            sentinel_selects.append(f"{double_expr} AS {semantic_name}")

    typed_sql = f"""
        SELECT
            buoy_id,
            datetime,
            {", ".join(sentinel_selects)}
        FROM ({datetime_sql}) AS with_datetime
    """

    # Step 7: dedup on (buoy_id, datetime).
    deduped_sql = f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY buoy_id, datetime) AS _rn
            FROM ({typed_sql}) AS typed
        ) AS ranked
        WHERE _rn = 1
    """

    # Step 8: final column order -- the Postgres/Parquet contract.
    ordered_columns = ", ".join(OBSERVATION_COLUMNS)
    return f"SELECT {ordered_columns} FROM ({deduped_sql}) AS deduped"


def build_coordinates_sql(bronze_root: str) -> str:
    """Build the coordinates transform: a separate DIMENSION read, never
    joined into the fact table here (the buoy_id join is query-time, in
    Postgres -- same ADR as the Spark job). Dedups per buoy_id keeping an
    arbitrary non-null lat/lon via ANY_VALUE (DuckDB's ignore-nulls
    aggregate, equivalent to Spark's `F.first(col, ignorenulls=True)`); in
    practice each buoy has exactly one `part.parquet` bronze file, so this is
    a defensive no-op against stale duplicate partitions.
    """
    raw_sql = _read_bronze_sql(
        os.path.join(bronze_root, "coordinates", "**", "*.parquet"),
        {"buoy_id": "VARCHAR"},
    )
    return f"""
        SELECT
            buoy_id,
            ANY_VALUE(CAST(latitude AS DOUBLE)) AS latitude,
            ANY_VALUE(CAST(longitude AS DOUBLE)) AS longitude
        FROM ({raw_sql}) AS raw
        GROUP BY buoy_id
    """


def read_observations(
    con: duckdb.DuckDBPyConnection, bronze_root: str, year: str | None = None
) -> duckdb.DuckDBPyRelation:
    """Read + fully transform bronze observations into a DuckDB relation
    with the exact OBSERVATION_COLUMNS schema/order.

    `year` narrows the read to a single bronze hive year; `None` reads the
    whole tree.
    """
    return con.sql(build_observations_sql(con, bronze_root, year))


def read_coordinates(
    con: duckdb.DuckDBPyConnection, bronze_root: str
) -> duckdb.DuckDBPyRelation:
    """Read + transform bronze coordinates into a DuckDB relation with the
    exact COORDINATE_COLUMNS schema/order."""
    return con.sql(build_coordinates_sql(bronze_root))


# ---------------------------------------------------------------------------
# Postgres load
# ---------------------------------------------------------------------------


def _year_bounds(year: str) -> tuple[str, str]:
    return f"{year}-01-01", f"{int(year) + 1}-01-01"


def _valid_row_predicate(year: str) -> str:
    """Rows admissible into `year`'s partition.

    `datetime` is NOT NULL in Postgres but nullable coming out of the
    transform: `make_timestamp` returns NULL whenever any TRY_CAST of a
    malformed field fails. Without this filter the insert dies on a NOT NULL
    violation partway through, which is a confusing way to learn that one
    source row had a corrupt hour field.

    The range check is equally load-bearing: a row whose real datetime falls
    outside the partition it was globbed into would violate the partition's
    CHECK constraint and abort the swap.
    """
    low, high = _year_bounds(year)
    return (
        "buoy_id IS NOT NULL AND datetime IS NOT NULL "
        f"AND datetime >= TIMESTAMP '{low}' AND datetime < TIMESTAMP '{high}'"
    )


def _swap_partition_sql(year: str, staging: str, partition: str) -> str:
    """Atomic replacement of one year partition, as a single DO block.

    A DO block is one implicit transaction, so readers observe either the
    entire old partition or the entire new one -- never a gap, never a mix.
    The ATTACH is O(1) because the staging table already carries a CHECK
    matching the range bound (see the module docstring for measurements).

    The index rename at the end is not cosmetic. Indexes live in the SCHEMA
    namespace, not the table's, so `uq_{partition}` is still held by the
    OUTGOING partition while the incoming staging table is being built -- which
    is why the staging index is created under `uq_{staging}` and only takes the
    canonical name here, once the old partition (and its index) is gone. Table
    CONSTRAINT names do not need this dance: those are scoped per-table, so the
    range CHECK can reuse its name freely.
    """
    low, high = _year_bounds(year)
    return f"""
DO $swap$
BEGIN
    IF to_regclass('public.{partition}') IS NOT NULL THEN
        ALTER TABLE buoy_observations DETACH PARTITION {partition};
        DROP TABLE {partition};
    END IF;
    ALTER TABLE {staging} RENAME TO {partition};
    ALTER INDEX uq_{staging} RENAME TO uq_{partition};
    ALTER TABLE buoy_observations ATTACH PARTITION {partition}
        FOR VALUES FROM ('{low}') TO ('{high}');
END
$swap$;
"""


def load_observations_year(
    con: duckdb.DuckDBPyConnection,
    bronze_root: str,
    year: str,
    alias: str = pg.DEFAULT_ALIAS,
) -> tuple[int, int]:
    """Rebuild one year partition. Returns (rows_loaded, rows_rejected)."""
    staging = f"obs_stage_{year}"
    partition = f"buoy_observations_{year}"
    obs_sql = build_observations_sql(con, bronze_root, year)
    predicate = _valid_row_predicate(year)

    # One scan yields both totals, so rejects are counted rather than inferred.
    total, accepted = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE {predicate}) "
        f"FROM ({obs_sql}) AS transformed"
    ).fetchone()

    # A previous crashed run can leave staging behind; it is always disposable.
    pg.execute(con, f"DROP TABLE IF EXISTS {staging}", alias)
    # `LIKE` copies column order, types and NOT NULL from the parent, so the
    # positional insert below cannot drift from db/init.sql. Indexes are
    # deliberately NOT copied: building them after the bulk load is far
    # cheaper than maintaining them during it.
    pg.execute(
        con,
        f"CREATE TABLE {staging} (LIKE buoy_observations INCLUDING DEFAULTS)",
        alias,
    )
    # DuckDB caches the remote catalog at ATTACH; without this the staging
    # table it just created is invisible to `insert_into`.
    pg.refresh_catalog(con)

    con.sql(f"SELECT * FROM ({obs_sql}) AS transformed WHERE {predicate}").insert_into(
        f"{alias}.{staging}"
    )

    low, high = _year_bounds(year)
    pg.execute(
        con,
        f"ALTER TABLE {staging} ADD CONSTRAINT ck_{staging}_range "
        f"CHECK (datetime >= '{low}' AND datetime < '{high}')",
        alias,
    )
    # Named after the STAGING table, not the partition: the canonical name is
    # still held by the partition being replaced. `_swap_partition_sql` renames
    # it once that partition is dropped.
    pg.execute(
        con,
        f"CREATE UNIQUE INDEX uq_{staging} ON {staging} (buoy_id, datetime)",
        alias,
    )
    pg.execute(con, _swap_partition_sql(year, staging, partition), alias)
    # Bulk-loaded tables carry no statistics until autovacuum eventually
    # notices; an explicit ANALYZE means the very first query after the swap
    # gets a sane plan instead of a default-estimate seq scan.
    pg.execute(con, f"ANALYZE {partition}", alias)
    pg.refresh_catalog(con)

    return int(accepted), int(total - accepted)


def record_partition_load(
    con: duckdb.DuckDBPyConnection,
    year: str,
    fingerprint: str,
    rows_loaded: int,
    rows_rejected: int,
    alias: str = pg.DEFAULT_ALIAS,
) -> None:
    pg.execute(
        con,
        "INSERT INTO observation_partitions "
        "(year, bronze_fingerprint, rows_loaded, rows_rejected, loaded_at) "
        f"VALUES ({int(year)}, {pg.literal(fingerprint)}, {rows_loaded}, "
        f"{rows_rejected}, now()) "
        "ON CONFLICT (year) DO UPDATE SET "
        "bronze_fingerprint = EXCLUDED.bronze_fingerprint, "
        "rows_loaded = EXCLUDED.rows_loaded, "
        "rows_rejected = EXCLUDED.rows_rejected, "
        "loaded_at = EXCLUDED.loaded_at",
        alias,
    )


def loaded_fingerprints(
    con: duckdb.DuckDBPyConnection, alias: str = pg.DEFAULT_ALIAS
) -> dict[str, str]:
    rows = pg.query(
        con, "SELECT year, bronze_fingerprint FROM observation_partitions", alias
    )
    return {str(year): fingerprint for year, fingerprint in rows}


def load_coordinates(
    con: duckdb.DuckDBPyConnection, bronze_root: str, alias: str = pg.DEFAULT_ALIAS
) -> int:
    """Reload the coordinates dimension via staging + swap-in-transaction.

    This dimension is tiny (one row per station), so it needs no partitioning
    -- but it must still never appear empty to a reader. DELETE + INSERT inside
    one transaction achieves that under MVCC and, unlike TRUNCATE, takes only a
    ROW EXCLUSIVE lock, so concurrent readers are never blocked.
    """
    staging = "coord_stage"
    pg.execute(con, f"DROP TABLE IF EXISTS {staging}", alias)
    pg.execute(
        con, f"CREATE TABLE {staging} (LIKE buoy_coordinates INCLUDING DEFAULTS)", alias
    )
    pg.refresh_catalog(con)

    read_coordinates(con, bronze_root).insert_into(f"{alias}.{staging}")

    pg.execute(
        con,
        "BEGIN; "
        "DELETE FROM buoy_coordinates; "
        f"INSERT INTO buoy_coordinates SELECT * FROM {staging}; "
        "COMMIT;",
        alias,
    )
    (count,) = pg.query(con, "SELECT count(*) FROM buoy_coordinates", alias)[0]
    pg.execute(con, f"DROP TABLE IF EXISTS {staging}", alias)
    pg.refresh_catalog(con)
    return int(count)


def main() -> None:
    bronze_root = os.environ.get("BRONZE_ROOT", "data/bronze")
    force = os.environ.get("SILVER_FORCE", "").lower() in {"1", "true", "yes"}

    con = duckdb.connect()
    configure_duckdb(con)
    pg.attach(con, pg.PostgresSettings.from_env())

    tracker = pg.RunTracker(con, "silver")
    tracker.start()

    rows_loaded = rows_rejected = years_loaded = 0
    try:
        years, unusable = discover_bronze_years(bronze_root)
        if unusable:
            print(
                f"[silver] WARNING: skipping bronze partitions with unusable "
                f"year values {unusable} -- these files could not be routed to "
                f"a year partition. Check the spider's filename parsing."
            )
        if not years:
            raise RuntimeError(
                f"no bronze observation years found under {bronze_root!r}; "
                f"run the scrape stage first"
            )

        known = {} if force else loaded_fingerprints(con)
        for year in years:
            fingerprint = fingerprint_bronze_year(bronze_root, year)
            if known.get(year) == fingerprint:
                print(f"[silver] {year}: unchanged, skipping")
                continue

            loaded, rejected = load_observations_year(con, bronze_root, year)
            record_partition_load(con, year, fingerprint, loaded, rejected)
            years_loaded += 1
            rows_loaded += loaded
            rows_rejected += rejected
            suffix = f" ({rejected} rejected)" if rejected else ""
            print(f"[silver] {year}: loaded {loaded} rows{suffix}")

        coordinates = load_coordinates(con, bronze_root)
        print(
            f"[silver] done -- {rows_loaded} observation rows across "
            f"{years_loaded} year partition(s), {coordinates} coordinates"
        )
        tracker.finish(
            "ok",
            rows_loaded=rows_loaded,
            years_loaded=years_loaded,
            rows_rejected=rows_rejected,
        )
    except Exception:
        tracker.finish(
            "failed",
            rows_loaded=rows_loaded,
            years_loaded=years_loaded,
            rows_rejected=rows_rejected,
            error=traceback.format_exc(),
        )
        raise
    finally:
        pg.detach(con)


if __name__ == "__main__":
    sys.exit(main())
