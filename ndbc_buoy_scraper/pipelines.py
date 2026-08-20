# Item pipelines for the bronze ingestion stage.
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from pathlib import Path

import pandas as pd


class BronzeParquetPipeline:
    """Writes one Parquet file per scraped response into a Hive-partitioned
    bronze landing zone, keyed on (buoy_id, year) for observations and
    (buoy_id) for coordinates. Each item already carries a full buoy-year
    table (see spider `_get_data_from_url`), so the write happens directly on
    `process_item` — no fixed-size buffering is needed, and re-scraping the
    same source file simply overwrites its own partition file (idempotent).
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
            self._write_observations(item)
        elif item.get('kind') == 'coordinates':
            self._write_coordinates(item)

        return item

    def _write_observations(self, item: dict) -> None:
        partition_dir = (
            self.bronze_root
            / 'observations'
            / f"buoy_id={item['buoy_id']}"
            / f"year={item['year']}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(item['records'])
        df.to_parquet(partition_dir / f"{item['source_stem']}.parquet", index=False)

    def _write_coordinates(self, item: dict) -> None:
        partition_dir = self.bronze_root / 'coordinates' / f"buoy_id={item['buoy_id']}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame([{
            'latitude': item.get('latitude'),
            'longitude': item.get('longitude'),
        }])
        df.to_parquet(partition_dir / 'part.parquet', index=False)
