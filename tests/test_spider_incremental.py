"""Tests for the spider's incremental-crawl skip and URL bookkeeping.

The skip is what turns a full NDBC crawl (1,351 stations / 17,050 year files)
into ~1,350 requests per run, so its edge cases are worth pinning: skipping a
year that can still change would silently freeze the data, and failing to skip
closed years puts 17,000 avoidable requests on a public NOAA service.
"""

from __future__ import annotations

import datetime as dt
import os

import pandas as pd
import pytest

from ndbc_buoy_scraper.spiders.ndbc_standard_meterological_spider import (
    NDBCStandardMeterologicalSpider,
    source_identity,
)

DOWNLOAD_LINK = (
    '/view_text_file.php?filename=41004h2019.txt.gz&dir=data/historical/stdmet/'
)


class FakeSettings(dict):
    def get(self, key, default=None):
        return super().get(key, default)

    def getbool(self, key, default=False):
        return bool(super().get(key, default))


@pytest.fixture
def spider(tmp_path):
    instance = NDBCStandardMeterologicalSpider()
    instance.settings = FakeSettings(BRONZE_ROOT=str(tmp_path))
    return instance


def _bronze_dir(tmp_path, buoy_id, year):
    directory = tmp_path / "observations" / f"buoy_id={buoy_id}" / f"year={year}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _write_bronze_marker(tmp_path, buoy_id, year, stem):
    """Write a READABLE bronze parquet, i.e. what a healthy crawl leaves behind.

    This used to write `b""`. A zero-byte file is exactly the corruption case
    `_bronze_file_is_usable` now rejects, so asserting it counted as "already
    have" was pinning the bug rather than the behaviour.
    """
    df = pd.DataFrame([{"#YY": 2019, "MM": 1, "WVHT": 1.5}])
    df.to_parquet(_bronze_dir(tmp_path, buoy_id, year) / f"{stem}.parquet", index=False)


def _write_header_only_bronze(tmp_path, buoy_id, year, stem):
    """A buoy-year NDBC reported nothing for: real header, zero rows."""
    df = pd.DataFrame([], columns=["#YY", "MM", "WVHT"])
    df.to_parquet(_bronze_dir(tmp_path, buoy_id, year) / f"{stem}.parquet", index=False)


def _write_zero_column_bronze(tmp_path, buoy_id, year, stem):
    """The poison pill: valid Parquet, no columns. DuckDB cannot open it."""
    pd.DataFrame([]).to_parquet(
        _bronze_dir(tmp_path, buoy_id, year) / f"{stem}.parquet", index=False
    )


class TestSourceIdentity:
    def test_parses_stem_and_year_from_ndbc_filename(self):
        assert source_identity(DOWNLOAD_LINK) == ("41004h2019", "2019")

    def test_missing_filename_falls_back_to_a_stable_url_hash(self):
        stem, year = source_identity('/view_text_file.php?dir=data/historical/')
        assert year == "unknown"
        assert stem.startswith("unknown-")
        # Stable, so a malformed response always lands in the same partition
        # instead of accumulating a new file on every crawl.
        assert stem == source_identity('/view_text_file.php?dir=data/historical/')[0]

    def test_distinct_urls_hash_distinctly(self):
        first, _ = source_identity('/view_text_file.php?dir=a')
        second, _ = source_identity('/view_text_file.php?dir=b')
        assert first != second


class TestIncrementalSkip:
    def test_closed_year_already_in_bronze_is_skipped(self, spider, tmp_path):
        _write_bronze_marker(tmp_path, "41004", "2019", "41004h2019")
        assert spider._already_have("41004", "2019", "41004h2019") is True

    def test_closed_year_not_in_bronze_is_fetched(self, spider):
        assert spider._already_have("41004", "2019", "41004h2019") is False

    def test_current_year_is_always_refetched(self, spider, tmp_path):
        """NDBC keeps rewriting the in-progress year's file.

        Treating it like a closed year would freeze the dataset at whatever was
        captured the first time -- and because the crawl would still succeed,
        nothing would look broken.
        """
        current = str(dt.date.today().year)
        _write_bronze_marker(tmp_path, "41004", current, f"41004h{current}")
        assert spider._already_have("41004", current, f"41004h{current}") is False

    def test_unknown_year_is_always_refetched(self, spider, tmp_path):
        _write_bronze_marker(tmp_path, "41004", "unknown", "unknown-abc123")
        assert spider._already_have("41004", "unknown", "unknown-abc123") is False

    def test_full_refresh_setting_disables_skipping(self, spider, tmp_path):
        _write_bronze_marker(tmp_path, "41004", "2019", "41004h2019")
        spider.settings["NDBC_FULL_REFRESH"] = True
        assert spider._already_have("41004", "2019", "41004h2019") is False

    def test_skip_path_matches_the_pipeline_write_path(self, spider, tmp_path):
        """The check and the writer must agree on the bronze path.

        BronzeParquetPipeline writes
        `observations/buoy_id=<id>/year=<year>/<source_stem>.parquet`; if this
        drifts, the skip consults a path nothing ever creates and incremental
        crawling silently stops working while still appearing to.
        """
        path = spider._bronze_path("41004", "2019", "41004h2019")
        assert path == os.path.join(
            str(tmp_path), "observations", "buoy_id=41004", "year=2019",
            "41004h2019.parquet",
        )


class TestUnreadableBronzeIsRefetched:
    """A closed year is never re-fetched, so an unreadable bronze file there is
    permanent: silver fails on it every run and nothing ever replaces it. The
    skip must therefore test readability, not mere existence.
    """

    def test_zero_column_parquet_is_refetched(self, spider, tmp_path):
        _write_zero_column_bronze(tmp_path, "47072", "2003", "47072h2003")
        assert spider._already_have("47072", "2003", "47072h2003") is False

    def test_zero_byte_file_is_refetched(self, spider, tmp_path):
        path = _bronze_dir(tmp_path, "47072", "2003") / "47072h2003.parquet"
        path.write_bytes(b"")
        assert spider._already_have("47072", "2003", "47072h2003") is False

    def test_truncated_parquet_is_refetched(self, spider, tmp_path):
        """A run killed mid-write leaves a file with no readable footer."""
        _write_bronze_marker(tmp_path, "47072", "2003", "47072h2003")
        path = _bronze_dir(tmp_path, "47072", "2003") / "47072h2003.parquet"
        path.write_bytes(path.read_bytes()[: path.stat().st_size // 2])
        assert spider._already_have("47072", "2003", "47072h2003") is False

    def test_header_only_parquet_counts_as_already_have(self, spider, tmp_path):
        """0 rows is legitimate data, not corruption.

        NDBC genuinely has buoy-years with no reports. DuckDB reads a 0-row
        file fine, so re-fetching it every run would be pure waste.
        """
        _write_header_only_bronze(tmp_path, "47072", "2003", "47072h2003")
        assert spider._already_have("47072", "2003", "47072h2003") is True


class TestUrlBookkeeping:
    def test_seen_urls_are_per_instance_not_shared(self):
        """`_urls_seen` used to be a mutable CLASS attribute.

        Shared across every spider instance in a process, a second crawl would
        skip everything the first one had already fetched and write no bronze
        at all.
        """
        first = NDBCStandardMeterologicalSpider()
        second = NDBCStandardMeterologicalSpider()
        first._urls_seen.add("https://example.org/a")

        assert "https://example.org/a" not in second._urls_seen

    def test_seen_urls_is_a_set(self):
        """Membership was O(n) over a list, quadratic across 17,050 URLs."""
        assert isinstance(NDBCStandardMeterologicalSpider()._urls_seen, set)
