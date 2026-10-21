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

import json

import duckdb
import pyarrow.parquet as pq
import pytest
from scrapy.http import TextResponse

from ndbc_buoy_scraper.malformed import format_report, load_records
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

# Reproduces aban6h2006.txt line 3474 verbatim in shape: NDBC dropped the
# newline between two records, so the first record's trailing field and the
# second's leading year fused into "1013.22003" -- 15 fields on a line the
# 8-column header cannot accept.
FUSED_LINE = (
    HEADER
    + '2003 01 01 00 30  180  1.5 1013.2\n'
    + '2003 01 01 01 30  190  1.6 1013.22003 01 01 02 30  200  1.7 1013.9\n'
    + '2003 01 01 03 30  210  1.8 1014.1\n'
)


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


def _scrape_all(spider, body: str) -> list[dict]:
    """Every item the spider's decode yields for `body`."""
    response = TextResponse(url=DATA_URL, body=body, encoding='utf-8')
    return list(spider._get_data_from_url(response, '47072'))


def _scrape(spider, body: str) -> dict:
    """The observations item, which a clean response yields on its own.

    A corrupt response yields a second `malformed` quarantine item alongside
    it; tests that care about that one use `_scrape_all`.
    """
    items = [i for i in _scrape_all(spider, body) if i['kind'] == 'observations']
    assert len(items) == 1
    return items[0]


def _malformed(spider, body: str) -> dict:
    """The quarantine item a corrupt response yields."""
    items = [i for i in _scrape_all(spider, body) if i['kind'] == 'malformed']
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


class TestCorruptUpstreamFile:
    """The aban6h2006 case: NDBC serves a file with a missing newline.

    The failure this guards is not the lost line -- it is the blast radius.
    A ParserError here aborts the callback, so the buoy-year never reaches
    bronze, the incremental skip never closes it, and every later run dies at
    the same byte. Two dropped observations beat a permanently wedged year.
    """

    def test_decode_survives_a_fused_line(self, spider):
        item = _scrape(spider, FUSED_LINE)
        # Both intact records kept; only the fused line is unrecoverable.
        assert len(item['records']) == 2
        assert item['columns'] == ['#YY', 'MM', 'DD', 'hh', 'mm', 'WDIR', 'WVHT', 'PRES']

    def test_drop_is_counted_not_silent(self, spider):
        _scrape(spider, FUSED_LINE)
        assert spider.crawler.stats.values['ndbc/malformed_lines_dropped'] == 1
        assert spider.crawler.stats.values['ndbc/files_with_malformed_lines'] == 1

    def test_clean_file_reports_no_drops(self, spider):
        _scrape(spider, WITH_ROWS)
        assert 'ndbc/malformed_lines_dropped' not in spider.crawler.stats.values

    def test_written_file_is_readable_by_duckdb(self, spider, pipeline, tmp_path):
        pipeline.process_item(_scrape(spider, FUSED_LINE), spider)

        path = tmp_path / 'observations' / 'buoy_id=47072' / 'year=2003' / '47072h2003.parquet'
        con = duckdb.connect()
        assert con.execute(f'SELECT count(*) FROM ({_select(path)})').fetchone() == (2,)


class TestQuarantineRecord:
    """What the crawl hands a human so the dropped lines can be recovered.

    A URL alone is not actionable: re-fetching an 8,000-line file to hunt for
    one fused record is not manual validation. The record must pin the line
    down.
    """

    def test_corrupt_response_yields_a_quarantine_item(self, spider):
        items = _scrape_all(spider, FUSED_LINE)
        assert [i['kind'] for i in items] == ['malformed', 'observations']

    def test_clean_response_yields_no_quarantine_item(self, spider):
        items = _scrape_all(spider, WITH_ROWS)
        assert [i['kind'] for i in items] == ['observations']

    def test_record_carries_the_url_to_refetch(self, spider):
        report = _malformed(spider, FUSED_LINE)
        assert report['url'] == DATA_URL
        assert report['buoy_id'] == '47072'
        assert report['year'] == '2003'
        assert report['source_stem'] == '47072h2003'

    def test_record_pins_the_exact_line(self, spider):
        report = _malformed(spider, FUSED_LINE)
        assert report['dropped_lines'] == 1
        assert report['located_lines'] == 1
        # Line 3 of the physical file, header included -- the number `sed -n
        # '3p'` and pandas' own ParserError both use.
        assert report['bad_lines'] == [{
            'line_number': 3,
            'field_count': 15,
            'text': '2003 01 01 01 30  190  1.6 1013.22003 01 01 02 30  200  1.7 1013.9',
        }]

    def test_short_lines_are_not_reported_as_dropped(self, spider):
        """pandas pads a short line with NaN; it is kept, so it is not a defect."""
        items = _scrape_all(spider, HEADER + '2003 01 01 00 30  180\n')
        assert [i['kind'] for i in items] == ['observations']

    def test_pipeline_writes_one_json_per_source(self, spider, pipeline, tmp_path):
        for item in _scrape_all(spider, FUSED_LINE):
            pipeline.process_item(item, spider)

        path = tmp_path / '_malformed' / '47072h2003.json'
        assert path.exists()
        record = json.loads(path.read_text())
        assert record['url'] == DATA_URL
        assert record['bad_lines'][0]['line_number'] == 3
        # `kind` is routing metadata for the pipeline, not part of the report.
        assert 'kind' not in record

    def test_rescraping_overwrites_instead_of_duplicating(self, spider, pipeline, tmp_path):
        """Mirrors the Parquet partitions: a re-crawl must not stack duplicates."""
        for _ in range(2):
            spider._urls_seen.clear()
            for item in _scrape_all(spider, FUSED_LINE):
                pipeline.process_item(item, spider)

        assert list((tmp_path / '_malformed').glob('*.json')) == [
            tmp_path / '_malformed' / '47072h2003.json'
        ]

    def test_quarantine_dir_is_invisible_to_the_silver_globs(self, spider, pipeline, tmp_path):
        """`_malformed/` sits beside the Hive trees silver reads, not inside them."""
        for item in _scrape_all(spider, FUSED_LINE):
            pipeline.process_item(item, spider)

        assert list(tmp_path.glob('observations/**/*.parquet'))
        assert not list(tmp_path.glob('observations/**/*.json'))
        assert not list(tmp_path.glob('coordinates/**/*.json'))


class TestMalformedReport:
    """`make malformed` -- the read-back path the human actually uses."""

    def test_reports_nothing_when_bronze_is_clean(self, tmp_path):
        assert 'No malformed source files' in format_report(
            load_records(str(tmp_path)), full=False
        )

    def test_missing_bronze_root_is_not_an_error(self, tmp_path):
        assert load_records(str(tmp_path / 'does-not-exist')) == []

    def test_summary_shows_url_and_line_numbers(self, spider, pipeline, tmp_path):
        for item in _scrape_all(spider, FUSED_LINE):
            pipeline.process_item(item, spider)

        report = format_report(load_records(str(tmp_path)), full=False)
        assert DATA_URL in report
        assert 'lines: 3' in report
        # The raw text is the FULL=1 payload, not the summary's.
        assert '1013.22003' not in report

    def test_full_mode_prints_the_dropped_line(self, spider, pipeline, tmp_path):
        for item in _scrape_all(spider, FUSED_LINE):
            pipeline.process_item(item, spider)

        report = format_report(load_records(str(tmp_path)), full=True)
        assert '1013.22003' in report
        assert '15 fields, expected 8' in report

    def test_unreadable_record_does_not_hide_the_others(self, spider, pipeline, tmp_path):
        for item in _scrape_all(spider, FUSED_LINE):
            pipeline.process_item(item, spider)
        (tmp_path / '_malformed' / 'truncated.json').write_text('{"url": ')

        # The good record still comes back; the bad one is reported, not raised.
        records = load_records(str(tmp_path))
        assert [r['source_stem'] for r in records] == ['47072h2003']


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
