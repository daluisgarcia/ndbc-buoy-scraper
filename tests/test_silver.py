"""Tests for ndbc_buoy_scraper/silver.py (originally the DuckDB port of a
since-removed PySpark implementation; silver.py is now the ONLY bronze->silver
ETL) against synthetic bronze-shaped NDBC fixtures.

Runs entirely on the HOST, no Docker/Postgres required:

    uv run pytest tests/test_silver.py

Reuses the fixture text and `write_bronze_observations` helper from
tests/fixtures/ndbc_snippets.py / tests/conftest.py.

The Postgres truncate-and-reload path (silver.load_postgres) is
deliberately NOT exercised here: it needs a live Postgres and is only
correct-by-inspection at this stage (see silver.py's docstring). No test in
this file calls it.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest

from ndbc_buoy_scraper import silver
from tests.conftest import write_bronze_observations
from tests.fixtures.ndbc_snippets import (
    DUPLICATE_DATETIME,
    HASH_YY_WITH_SENTINELS,
    MINUTE_ABSENT,
    NEWER_COLUMN_PRESENT,
    YY_TWO_DIGIT_HEADER,
    YYYY_HEADER,
)

# Dedicated fixture proving month (MM) and minute (mm) are never confused:
# MM=05, DD=09, hh=03, mm=47 -- if the DuckDB reader's case-insensitive
# column de-duplication (see silver._find_minute_column) were mishandled,
# the minute would silently resolve to the month column's value (05)
# instead of 47, or vice versa. Any mix-up here fails loudly.
MM_MM_DISTINCT = """
#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
#yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi ft
2023 05 09 03 47 100 5.0 6.0 1.0 5.0 4.0 110 1010.0 15.0 14.0 13.0 9.0 1.0
"""


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


def _rows(con, bronze_root):
    return silver.read_observations(con, str(bronze_root)).fetchall()


def _columns(con, bronze_root):
    return silver.read_observations(con, str(bronze_root)).columns


class TestPartitionDiscovery:
    def test_buoy_id_recovered_from_path(self, con, tmp_path):
        write_bronze_observations(tmp_path, "99999", "2020", MINUTE_ABSENT)
        result = silver.read_observations(con, str(tmp_path)).fetchall()
        assert len(result) == 1
        assert result[0][0] == "99999"


class TestUnitsRowDropped:
    def test_units_row_is_filtered_out(self, con, tmp_path):
        # HASH_YY_WITH_SENTINELS carries a units row ("#yr mo dy ...") ahead
        # of its 2 real data rows; only the 2 data rows must survive.
        write_bronze_observations(tmp_path, "41001", "2020", HASH_YY_WITH_SENTINELS)
        result = silver.read_observations(con, str(tmp_path)).fetchall()
        assert len(result) == 2


class TestYearHeaderVariants:
    def test_hash_yy_header_resolves_to_four_digit_year(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", HASH_YY_WITH_SENTINELS)
        columns = _columns(con, tmp_path)
        dt_idx = columns.index("datetime")
        years = {row[dt_idx].year for row in _rows(con, tmp_path)}
        assert years == {2020}

    def test_yyyy_header_resolves_to_four_digit_year(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41002", "2015", YYYY_HEADER)
        columns = _columns(con, tmp_path)
        dt_idx = columns.index("datetime")
        years = {row[dt_idx].year for row in _rows(con, tmp_path)}
        assert years == {2015}

    def test_two_digit_yy_header_pivots_at_50(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41003", "unknown", YY_TWO_DIGIT_HEADER)
        columns = _columns(con, tmp_path)
        dt_idx = columns.index("datetime")
        years = sorted(row[dt_idx].year for row in _rows(con, tmp_path))
        # "79" (>= 50) -> 1979; "23" (< 50) -> 2023.
        assert years == [1979, 2023]


class TestMinuteFolding:
    def test_minute_present_is_folded_into_datetime(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", HASH_YY_WITH_SENTINELS)
        columns = _columns(con, tmp_path)
        dt_idx = columns.index("datetime")
        datetimes = sorted(row[dt_idx] for row in _rows(con, tmp_path))
        assert datetimes[0] == dt.datetime(2020, 1, 15, 10, 30)
        assert datetimes[1] == dt.datetime(2020, 1, 15, 11, 0)

    def test_minute_absent_defaults_to_zero(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41004", "2020", MINUTE_ABSENT)
        columns = _columns(con, tmp_path)
        dt_idx = columns.index("datetime")
        result = _rows(con, tmp_path)
        assert len(result) == 1
        assert result[0][dt_idx] == dt.datetime(2020, 3, 10, 5, 0)


class TestMinuteMonthDistinctness:
    """Pins the CRITICAL GOTCHA fix: DuckDB's read_parquet folds `MM` and
    `mm` case-insensitively for collision detection and silently renames
    the later one (mm -> mm_1) rather than raising or merging -- see
    silver._find_minute_column's docstring."""

    def test_month_and_minute_are_never_confused(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41009", "2023", MM_MM_DISTINCT)
        columns = _columns(con, tmp_path)
        dt_idx = columns.index("datetime")
        result = _rows(con, tmp_path)
        assert len(result) == 1
        assert result[0][dt_idx] == dt.datetime(2023, 5, 9, 3, 47)


class TestSchemaDrift:
    def test_newer_column_present_and_absent_across_files(self, con, tmp_path):
        # MINUTE_ABSENT has no PTDY column at all; NEWER_COLUMN_PRESENT does.
        # union_by_name must union them without raising, filling the
        # missing side with NULL (spec scenario "Newer columns present vs
        # absent").
        write_bronze_observations(tmp_path, "41005", "2020", MINUTE_ABSENT)
        write_bronze_observations(tmp_path, "41006", "2021", NEWER_COLUMN_PRESENT)
        columns = _columns(con, tmp_path)
        buoy_idx = columns.index("buoy_id")
        ptdy_idx = columns.index("pressure_tendency")
        rows = {row[buoy_idx]: row for row in _rows(con, tmp_path)}
        assert rows["41006"][ptdy_idx] == pytest.approx(0.5)
        assert rows["41005"][ptdy_idx] is None


class TestSentinelNullification:
    def test_sentinel_nulled_and_non_sentinel_preserved(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", HASH_YY_WITH_SENTINELS)
        columns = _columns(con, tmp_path)

        def col(row, name):
            return row[columns.index(name)]

        rows = sorted(_rows(con, tmp_path), key=lambda r: col(r, "datetime"))
        first, second = rows[0], rows[1]

        # First row (10:30): WDIR=099 is a LEGITIMATE 99-degree reading --
        # the wind_direction sentinel is 999.0, not 99.0 -- must survive.
        assert col(first, "wind_direction") == pytest.approx(99.0)
        # WSPD=99.0 IS the wind_speed sentinel -- must become NULL. Same raw
        # sentinel value (99.0) as WDIR's legitimate reading above: proves
        # nulling is per-column, not a blanket 99.0 replace.
        assert col(first, "wind_speed") is None

        # Second row (11:00): every sentinel-bearing column holds its
        # documented sentinel value and must all null out.
        assert col(second, "wind_direction") is None  # 999
        assert col(second, "mean_wave_direction") is None  # 999
        assert col(second, "pressure") is None  # 9999.0
        assert col(second, "air_temperature") is None  # 999.0
        assert col(second, "sea_surface_temperature") is None  # 999.0
        assert col(second, "dew_point_temperature") is None  # 999.0
        assert col(second, "visibility") is None  # 99.0
        assert col(second, "tide") is None  # 99.00 == 99.0
        # Non-sentinel values on the same row must survive untouched.
        assert col(second, "wind_speed") == pytest.approx(8.0)
        assert col(second, "gust") == pytest.approx(9.0)


class TestDedup:
    def test_dedup_removes_duplicate_buoy_id_datetime(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41007", "2022", DUPLICATE_DATETIME)
        result = silver.read_observations(con, str(tmp_path)).fetchall()
        assert len(result) == 1


class TestCoordinates:
    def test_coordinates_projected_to_canonical_columns(self, con, tmp_path):
        import os

        import pandas as pd

        partition_dir = os.path.join(str(tmp_path), "coordinates", "buoy_id=41001")
        os.makedirs(partition_dir, exist_ok=True)
        pd.DataFrame([{"latitude": 12.3, "longitude": -45.6}]).to_parquet(
            os.path.join(partition_dir, "part.parquet"), index=False
        )

        rel = silver.read_coordinates(con, str(tmp_path))
        assert rel.columns == silver.COORDINATE_COLUMNS
        rows = rel.fetchall()
        assert rows == [("41001", 12.3, -45.6)]
