"""Tests for the per-year partition load strategy in silver.py.

Covers the pure/host-side half: year discovery, bronze fingerprinting, the
year-scoped read, the partition admission predicate, and the generated swap
SQL. The Postgres round trip itself is exercised by running the real pipeline
(`make silver`) -- these tests deliberately need no database so the suite stays
runnable with a bare `uv run pytest`.
"""

from __future__ import annotations

import datetime as dt
import os
import time

import duckdb
import pytest

from ndbc_buoy_scraper import silver
from tests.conftest import write_bronze_observations
from tests.fixtures.ndbc_snippets import HASH_YY_WITH_SENTINELS, MINUTE_ABSENT


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


class TestYearDiscovery:
    def test_finds_four_digit_years_across_buoys(self, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", HASH_YY_WITH_SENTINELS)
        write_bronze_observations(tmp_path, "41002", "2021", MINUTE_ABSENT)
        write_bronze_observations(tmp_path, "41003", "2020", MINUTE_ABSENT)

        years, unusable = silver.discover_bronze_years(str(tmp_path))

        assert years == ["2020", "2021"]
        assert unusable == []

    def test_unusable_year_values_are_reported_not_silently_dropped(self, tmp_path):
        # The spider falls back to year=unknown when a source filename carries
        # no parseable year. Those must surface, because a silent skip looks
        # exactly like "there was nothing to load".
        write_bronze_observations(tmp_path, "41001", "2020", MINUTE_ABSENT)
        write_bronze_observations(tmp_path, "41004", "unknown", MINUTE_ABSENT)

        years, unusable = silver.discover_bronze_years(str(tmp_path))

        assert years == ["2020"]
        assert unusable == ["unknown"]

    def test_empty_bronze_yields_nothing(self, tmp_path):
        assert silver.discover_bronze_years(str(tmp_path)) == ([], [])


class TestFingerprinting:
    def test_stable_when_bronze_untouched(self, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", MINUTE_ABSENT)
        first = silver.fingerprint_bronze_year(str(tmp_path), "2020")
        assert first == silver.fingerprint_bronze_year(str(tmp_path), "2020")

    def test_changes_when_a_file_is_rewritten(self, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", MINUTE_ABSENT)
        before = silver.fingerprint_bronze_year(str(tmp_path), "2020")

        # mtime has sub-second resolution but not infinite resolution; sleep
        # past it so this asserts real behaviour rather than clock luck.
        time.sleep(0.05)
        write_bronze_observations(tmp_path, "41001", "2020", HASH_YY_WITH_SENTINELS)

        assert silver.fingerprint_bronze_year(str(tmp_path), "2020") != before

    def test_changes_when_a_new_buoy_appears_in_the_year(self, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", MINUTE_ABSENT)
        before = silver.fingerprint_bronze_year(str(tmp_path), "2020")

        write_bronze_observations(tmp_path, "41002", "2020", MINUTE_ABSENT)

        assert silver.fingerprint_bronze_year(str(tmp_path), "2020") != before

    def test_missing_year_is_marked_empty(self, tmp_path):
        assert silver.fingerprint_bronze_year(str(tmp_path), "1999") == "empty"


class TestYearScopedRead:
    def test_read_is_narrowed_to_one_year(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", HASH_YY_WITH_SENTINELS)
        write_bronze_observations(tmp_path, "41002", "2021", MINUTE_ABSENT)

        rel = silver.read_observations(con, str(tmp_path), year="2021")
        rows = rel.fetchall()

        assert {row[0] for row in rows} == {"41002"}

    def test_unscoped_read_still_sees_every_year(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", HASH_YY_WITH_SENTINELS)
        write_bronze_observations(tmp_path, "41002", "2021", MINUTE_ABSENT)

        rows = silver.read_observations(con, str(tmp_path)).fetchall()

        assert {row[0] for row in rows} == {"41001", "41002"}

    def test_year_glob_targets_the_hive_directory(self, tmp_path):
        glob = silver.observations_glob(str(tmp_path), "2020")
        assert glob.endswith(os.path.join("*", "year=2020", "*.parquet"))


class TestPartitionAdmission:
    """The predicate guarding each year's partition.

    Both halves are load-bearing: Postgres declares `datetime NOT NULL`, and
    the staging table carries a CHECK matching its range bound. A row failing
    either would abort the insert or the swap.
    """

    def _accepted(self, con, tmp_path, year):
        sql = silver.build_observations_sql(con, str(tmp_path), year)
        predicate = silver._valid_row_predicate(year)
        return con.execute(
            f"SELECT count(*) FROM ({sql}) AS t WHERE {predicate}"
        ).fetchone()[0]

    def test_rows_inside_the_year_are_admitted(self, con, tmp_path):
        write_bronze_observations(tmp_path, "41001", "2020", HASH_YY_WITH_SENTINELS)
        assert self._accepted(con, tmp_path, "2020") == 2

    def test_null_datetime_is_rejected(self, con, tmp_path):
        # An unparseable hour makes make_timestamp return NULL. Postgres would
        # reject it with a NOT NULL violation halfway through the load.
        corrupt = """
#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
#yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi ft
2020 01 15 XX 30 099 99.0 12.0 1.5 7.0 5.5 100 1013.2 20.1 22.3 18.0 10.0 1.5
"""
        write_bronze_observations(tmp_path, "41001", "2020", corrupt)
        assert self._accepted(con, tmp_path, "2020") == 0

    def test_row_outside_the_partition_range_is_rejected(self, con, tmp_path):
        # A 2019 reading sitting in the year=2020 bronze partition would
        # violate the staging CHECK and abort the ATTACH.
        misfiled = """
#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS TIDE
#yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi ft
2019 12 31 23 30 099 8.0 12.0 1.5 7.0 5.5 100 1013.2 20.1 22.3 18.0 10.0 1.5
2020 01 01 00 30 099 8.0 12.0 1.5 7.0 5.5 100 1013.2 20.1 22.3 18.0 10.0 1.5
"""
        write_bronze_observations(tmp_path, "41001", "2020", misfiled)
        assert self._accepted(con, tmp_path, "2020") == 1


class TestSwapSql:
    def test_swap_declares_the_check_before_attaching(self):
        """The CHECK is what makes ATTACH O(1) instead of O(partition).

        Measured against Postgres 16 on a 1M-row partition: 0.8 ms with the
        constraint pre-declared, 33 ms without, growing with row count while
        holding ACCESS EXCLUSIVE. `load_observations_year` adds the constraint
        before calling this SQL; the range bounds must agree exactly or
        Postgres falls back to a full validation scan.
        """
        sql = silver._swap_partition_sql("2020", "obs_stage_2020", "buoy_observations_2020")

        assert "FOR VALUES FROM ('2020-01-01') TO ('2021-01-01')" in sql
        assert "DETACH PARTITION buoy_observations_2020" in sql
        assert "ALTER TABLE obs_stage_2020 RENAME TO buoy_observations_2020" in sql
        # A DO block is one implicit transaction -- that atomicity is why
        # readers never observe a gap between detach and attach.
        assert sql.strip().startswith("DO $swap$")

    def test_index_is_renamed_only_after_the_old_partition_is_dropped(self):
        """Indexes share the SCHEMA namespace, so the canonical index name is
        still held by the outgoing partition while staging is built.

        Creating the staging index under the canonical name fails on every
        rebuild after the first with `relation "uq_..." already exists` -- a
        path the initial load cannot reveal, because there is no old partition
        to collide with.
        """
        sql = silver._swap_partition_sql("2020", "obs_stage_2020", "buoy_observations_2020")

        drop_at = sql.index("DROP TABLE buoy_observations_2020")
        rename_at = sql.index("ALTER INDEX uq_obs_stage_2020 RENAME TO uq_buoy_observations_2020")
        attach_at = sql.index("ATTACH PARTITION")

        assert drop_at < rename_at < attach_at

    def test_bounds_match_the_admission_predicate(self):
        """Swap bounds and admission predicate must never diverge."""
        low, high = silver._year_bounds("2020")
        predicate = silver._valid_row_predicate("2020")
        swap = silver._swap_partition_sql("2020", "s", "p")

        assert low in predicate and high in predicate
        assert f"FROM ('{low}') TO ('{high}')" in swap


class TestResourceCaps:
    def test_limits_are_applied_to_the_connection(self, con, tmp_path):
        """DuckDB defaults to ~80% of system RAM and would starve the
        co-resident Postgres it loads into."""
        silver.configure_duckdb(
            con,
            memory_limit="512MB",
            threads=2,
            temp_directory=str(tmp_path / "duck-tmp"),
        )

        settings = dict(
            con.execute(
                "SELECT name, value FROM duckdb_settings() "
                "WHERE name IN ('memory_limit', 'threads', 'preserve_insertion_order')"
            ).fetchall()
        )

        assert settings["threads"] == "2"
        assert settings["preserve_insertion_order"] == "false"
        assert os.path.isdir(tmp_path / "duck-tmp")

        # DuckDB reads "MB" as DECIMAL megabytes and reports back in MiB, so
        # 512MB surfaces as 488.2 MiB. Worth pinning: it means the production
        # DUCKDB_MEMORY_LIMIT=1500MB is ~1430 MiB, and anyone sizing the 6 GB
        # box against `free -m` output needs that 5% gap to be explicit rather
        # than discovered.
        assert settings["memory_limit"] == "488.2 MiB"


class TestYearBounds:
    @pytest.mark.parametrize(
        "year,expected",
        [("1978", ("1978-01-01", "1979-01-01")), ("2025", ("2025-01-01", "2026-01-01"))],
    )
    def test_bounds_span_exactly_one_calendar_year(self, year, expected):
        assert silver._year_bounds(year) == expected

    def test_bounds_are_half_open_so_partitions_never_overlap(self):
        _, high_2020 = silver._year_bounds("2020")
        low_2021, _ = silver._year_bounds("2021")
        # Postgres range partitions are inclusive-lower/exclusive-upper, so the
        # shared boundary belongs to 2021 alone.
        assert high_2020 == low_2021
        assert dt.date.fromisoformat(high_2020) == dt.date(2021, 1, 1)
