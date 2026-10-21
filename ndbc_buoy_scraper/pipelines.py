# Item pipelines for the bronze ingestion stage.
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import json
from pathlib import Path

import pandas as pd

# Sidecar directory under the bronze root holding one JSON record per source
# file NDBC served corrupt. Leading underscore so it sorts away from the
# `observations/` and `coordinates/` Hive trees and can never be mistaken for
# a partition key by a `read_parquet` glob.
QUARANTINE_DIR = '_malformed'


class BronzeParquetPipeline:
    """Writes one Parquet file per scraped response into a Hive-partitioned
    bronze landing zone, keyed on (buoy_id, year) for observations and
    (buoy_id) for coordinates. Each item already carries a full buoy-year
    table (see spider `_get_data_from_url`), so the write happens directly on
    `process_item` — no fixed-size buffering is needed, and re-scraping the
    same source file simply overwrites its own partition file (idempotent).

    Every file written here must be READABLE by the silver stage, not merely
    present. A Parquet file with zero columns parses as valid but DuckDB
    refuses to open it, and the spider's skip check treats any bronze file as
    work already done — so one unreadable file permanently fails the year it
    sits in. Hence the schema handling in `_write_observations`.
    """

    def __init__(self, bronze_root: str):
        self.bronze_root = Path(bronze_root)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            bronze_root=crawler.settings.get('BRONZE_ROOT', 'data/bronze')
        )

    def process_item(self, item: dict, spider):
        if item.get('kind') == 'observations':
            self._write_observations(item, spider)
        elif item.get('kind') == 'coordinates':
            self._write_coordinates(item)
        elif item.get('kind') == 'malformed':
            self._write_malformed(item, spider)

        return item

    def _write_malformed(self, item: dict, spider) -> None:
        """Persist the quarantine record for one corrupt NDBC source file.

        This lands in bronze rather than in Postgres on purpose. `pipeline_runs`
        is best-effort monitoring the crawl is designed to survive without
        (TRACK_RUNS=0, an unreachable database), and this record is the ONLY
        surviving evidence of data the decode silently dropped. Putting the
        evidence of a silent loss behind an optional backend would reproduce
        the exact failure the record exists to prevent. Bronze is written by
        definition -- it is the stage's whole output.

        One file per source stem, mirroring the Parquet partitions: re-scraping
        the same buoy-year overwrites its own record instead of appending a
        duplicate on every run. A file that disappears from here because NDBC
        fixed its data is the correct outcome -- but only a full refresh
        rewrites it, since a closed year is otherwise never re-fetched.
        """
        quarantine_dir = self.bronze_root / QUARANTINE_DIR
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        path = quarantine_dir / f"{item['source_stem']}.json"
        # Written whole via a temp file and an atomic rename: a run killed
        # mid-write would otherwise leave truncated JSON that breaks the
        # DuckDB read of the WHOLE quarantine glob, hiding every other record.
        tmp_path = path.with_suffix('.json.tmp')
        payload = {k: v for k, v in item.items() if k != 'kind'}
        tmp_path.write_text(json.dumps(payload, indent=2, default=str), encoding='utf-8')
        tmp_path.replace(path)

        spider.logger.warning(f'quarantine record written: {path}')

    def _write_observations(self, item: dict, spider) -> None:
        columns = list(item.get('columns') or [])
        records = item['records']

        # No columns AND no rows means there is no schema to persist. Writing
        # anyway produces a zero-column Parquet: valid on disk, unopenable by
        # DuckDB, and — because the spider's skip check would count it as
        # already fetched — never replaced. An absent file is strictly better:
        # it gets re-fetched next run instead of failing the year forever.
        if not columns and not records:
            spider.logger.warning(
                f"no columns and no rows for buoy {item['buoy_id']} year "
                f"{item['year']} ({item['source_stem']}); skipping the bronze "
                f"write so the next crawl retries it"
            )
            spider.crawler.stats.inc_value('ndbc/bronze_skipped_no_schema')
            return

        partition_dir = (
            self.bronze_root
            / 'observations'
            / f"buoy_id={item['buoy_id']}"
            / f"year={item['year']}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Passing `columns` restores the header that `to_dict(orient='records')`
        # drops when the frame has no rows, so a header-only NDBC file lands as
        # a readable 0-row Parquet instead of an unreadable 0-column one. With
        # rows present it is a no-op that also pins column order. `or None`
        # keeps the original behaviour for any item produced without the key.
        df = pd.DataFrame(records, columns=columns or None)
        df.to_parquet(partition_dir / f"{item['source_stem']}.parquet", index=False)

    def _write_coordinates(self, item: dict) -> None:
        partition_dir = self.bronze_root / 'coordinates' / f"buoy_id={item['buoy_id']}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame([{
            'latitude': item.get('latitude'),
            'longitude': item.get('longitude'),
        }])
        df.to_parquet(partition_dir / 'part.parquet', index=False)
