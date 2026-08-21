"""Tests for BronzeParquetPipeline's write contract.

The bronze writer's real obligation is not "a file exists" but "the silver
stage can read it". Those came apart once: a header-only NDBC response round
-tripped through `to_dict(orient='records')` lost its schema, landed as a
zero-column Parquet, and DuckDB refused to open it -- failing the entire year
partition, not just that buoy. These tests drive the whole spider -> pipeline
-> DuckDB path, because the bug lived in the seam between the two halves and
neither side could see it alone.
"""

from __future__ import annotations

import duckdb
import pyarrow.parquet as pq
import pytest
from scrapy.http import TextResponse

from ndbc_buoy_scraper.pipelines import BronzeParquetPipeline
from ndbc_buoy_scraper.spiders.ndbc_standard_meterological_spider import (
    NDBCStandardMeterologicalSpider,
)

DATA_URL = (
    'https://www.ndbc.noaa.gov/view_text_file.php'
    '?filename=47072h2003.txt.gz&dir=data/historical/stdmet/'
)

HEADER = '#YY  MM DD hh mm WDIR WVHT  PRES\n'
WITH_ROWS = HEADER + '2003 01 01 00 30  180  1.5 1013.2\n'


class FakeStats:
    def __init__(self):
        self.values: dict[str, int] = {}

    def inc_value(self, key, count=1, start=0):
        self.values[key] = self.values.get(key, start) + count


class FakeCrawler:
    def __init__(self):
        self.stats = FakeStats()


class FakeSettings(dict):
    def get(self, key, default=None):
        return super().get(key, default)

    def getbool(self, key, default=False):
        return bool(super().get(key, default))


@pytest.fixture
def spider(tmp_path):
    instance = NDBCStandardMeterologicalSpider()
    instance.settings = FakeSettings(BRONZE_ROOT=str(tmp_path))
    instance.crawler = FakeCrawler()
    return instance


@pytest.fixture
def pipeline(tmp_path):
    return BronzeParquetPipeline(bronze_root=str(tmp_path))


def _scrape(spider, body: str) -> dict:
    """Run the spider's decode on `body` and return the single item it yields."""
    response = TextResponse(url=DATA_URL, body=body, encoding='utf-8')
    items = list(spider._get_data_from_url(response, '47072'))
    assert len(items) == 1
    return items[0]


def _select(path) -> str:
    """Read one bronze file's OWN schema.

    `hive_partitioning=false` is required: pointing `read_parquet` at a path
    under `buoy_id=.../year=...` makes DuckDB auto-detect those directories and
    append them as columns, which would mask what the writer actually stored.
    """
    return f"SELECT * FROM read_parquet('{path}', hive_partitioning = false)"


def _stored_columns(path) -> list[str]:
    """Column names actually persisted in the file, read straight from Parquet.

    Kept separate from `_duckdb_columns` on purpose: DuckDB's reader folds
    identifiers case-insensitively and renames `mm` to `mm_1` when `MM` is also
    present (documented in silver.py::_find_minute_column). That is a reader
    artifact, so asserting the writer's output through DuckDB would conflate the
    two. pyarrow reports what was stored.
    """
    return list(pq.ParquetFile(path).schema_arrow.names)


def _duckdb_columns(path) -> list[str]:
    """Columns DuckDB sees, or raise exactly the way the silver stage would."""
    con = duckdb.connect()
    return [row[0] for row in con.execute(f'DESCRIBE {_select(path)}').fetchall()]


class TestHeaderOnlyResponse:
    """The 47072h2003 case: a real header, zero data rows."""

    def test_item_carries_columns_even_with_no_records(self, spider):
        item = _scrape(spider, HEADER)
        assert item['records'] == []
        # Without this the writer has nothing to rebuild the schema from.
        assert item['columns'] == ['#YY', 'MM', 'DD', 'hh', 'mm', 'WDIR', 'WVHT', 'PRES']

    def test_written_file_is_readable_by_duckdb(self, spider, pipeline, tmp_path):
        pipeline.process_item(_scrape(spider, HEADER), spider)

        path = tmp_path / 'observations' / 'buoy_id=47072' / 'year=2003' / '47072h2003.parquet'
        assert path.exists()
        assert _stored_columns(path) == [
            '#YY', 'MM', 'DD', 'hh', 'mm', 'WDIR', 'WVHT', 'PRES'
        ]
        # The regression: this used to raise InvalidInputException,
        # "Need at least one non-root column in the file", failing the whole year.
        assert len(_duckdb_columns(path)) == 8

    def test_written_file_has_no_rows(self, spider, pipeline, tmp_path):
        """Schema preserved, but no data invented."""
        pipeline.process_item(_scrape(spider, HEADER), spider)

        path = tmp_path / 'observations' / 'buoy_id=47072' / 'year=2003' / '47072h2003.parquet'
        con = duckdb.connect()
        assert con.execute(f'SELECT count(*) FROM ({_select(path)})').fetchone() == (0,)


class TestNormalResponse:
    def test_rows_and_columns_survive_unchanged(self, spider, pipeline, tmp_path):
        """Passing `columns` must not perturb the ordinary path."""
        pipeline.process_item(_scrape(spider, WITH_ROWS), spider)

        path = tmp_path / 'observations' / 'buoy_id=47072' / 'year=2003' / '47072h2003.parquet'
        assert _stored_columns(path) == [
            '#YY', 'MM', 'DD', 'hh', 'mm', 'WDIR', 'WVHT', 'PRES'
        ]

        con = duckdb.connect()
        assert con.execute(_select(path)).fetchall() == [
            (2003, 1, 1, 0, 30, 180, 1.5, 1013.2)
        ]


class TestSchemalessItemIsNotWritten:
    """Defence in depth: if an item somehow reaches the writer with neither
    columns nor rows, writing nothing beats writing an unopenable file. The
    spider's skip check counts any bronze file as done, so an absent file is
    retried next run while a bad one never is.
    """

    def test_no_file_is_created(self, pipeline, spider, tmp_path):
        pipeline.process_item(
            {
                'kind': 'observations',
                'buoy_id': '47072',
                'year': '2003',
                'source_stem': '47072h2003',
                'columns': [],
                'records': [],
            },
            spider,
        )
        assert not (tmp_path / 'observations').exists()

    def test_the_skip_is_counted_in_stats(self, pipeline, spider):
        pipeline.process_item(
            {
                'kind': 'observations',
                'buoy_id': '47072',
                'year': '2003',
                'source_stem': '47072h2003',
                'columns': [],
                'records': [],
            },
            spider,
        )
        assert spider.crawler.stats.values['ndbc/bronze_skipped_no_schema'] == 1

    def test_items_without_a_columns_key_still_write(self, pipeline, spider, tmp_path):
        """Backward compatibility for any producer predating `columns`."""
        pipeline.process_item(
            {
                'kind': 'observations',
                'buoy_id': '47072',
                'year': '2003',
                'source_stem': '47072h2003',
                'records': [{'#YY': 2003, 'WVHT': 1.5}],
            },
            spider,
        )
        path = tmp_path / 'observations' / 'buoy_id=47072' / 'year=2003' / '47072h2003.parquet'
        assert _stored_columns(path) == ['#YY', 'WVHT']
