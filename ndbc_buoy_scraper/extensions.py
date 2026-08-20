"""Scrapy extensions.

`PipelineRunTracker` writes each crawl into the same `pipeline_runs` table the
silver stage uses, so both halves of the pipeline are observable from one SQL
query instead of one table plus a pile of log files.

It connects through DuckDB's `postgres` extension rather than a dedicated
driver -- see ndbc_buoy_scraper/pg.py for why the project has no psycopg
dependency.
"""

from __future__ import annotations

import duckdb
from scrapy import signals
from scrapy.exceptions import NotConfigured

from ndbc_buoy_scraper import pg


class PipelineRunTracker:
    """Opens a `pipeline_runs` row at spider start and closes it at finish.

    Every Postgres interaction here is best-effort. Monitoring must never be
    able to fail a crawl: an unreachable database should cost you the run
    record, not 17,050 downloads.
    """

    def __init__(self):
        self.con: duckdb.DuckDBPyConnection | None = None
        self.tracker: pg.RunTracker | None = None

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool("TRACK_RUNS", True):
            raise NotConfigured("run tracking disabled via TRACK_RUNS")
        extension = cls()
        crawler.signals.connect(extension.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)
        return extension

    def spider_opened(self, spider):
        try:
            self.con = duckdb.connect()
            pg.attach(self.con, pg.PostgresSettings.from_env())
            self.tracker = pg.RunTracker(self.con, "scrape")
            self.tracker.start()
        except Exception as exc:  # noqa: BLE001 -- see class docstring
            spider.logger.warning(f"run tracking unavailable: {exc}")
            self.tracker = None

    def spider_closed(self, spider, reason):
        if self.tracker is not None:
            stats = spider.crawler.stats.get_stats()
            # Scrapy's own `finish_reason` is the authority on how the crawl
            # ended: 'finished' means the scheduler drained normally, while
            # 'memusage_exceeded', 'closespider_errorcount' and friends all
            # mean an incomplete bronze that must not be reported as 'ok'.
            status = "ok" if reason == "finished" else "failed"
            self.tracker.finish(
                status,
                rows_loaded=stats.get("item_scraped_count", 0),
                scrapy_stats=stats,
                error=None if status == "ok" else f"finish_reason={reason}",
            )
        if self.con is not None:
            try:
                pg.detach(self.con)
                self.con.close()
            except Exception as exc:  # noqa: BLE001 -- teardown is advisory
                # Debug, not warning: the run record is already written by this
                # point, so a failed teardown costs nothing and should not look
                # like a problem in the crawl's output.
                spider.logger.debug(f"run tracker teardown failed: {exc}")
