# Scrapy settings for ndbc_buoy_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os

BOT_NAME = "ndbc_buoy_scraper"

SPIDER_MODULES = ["ndbc_buoy_scraper.spiders"]
NEWSPIDER_MODULE = "ndbc_buoy_scraper.spiders"

ADDONS = {}

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# Identify the crawler honestly, with a contact address.
#
# This used to be a spoofed Chrome string -- AND, because of a trailing comma,
# it was a one-element TUPLE rather than a str. Beyond that bug: NDBC is a
# public NOAA service funded to serve this data, and a full crawl is 17,050
# file requests. An operator who needs to throttle or contact the client has
# no way to do so behind a forged desktop-browser UA. Set NDBC_CONTACT to a
# real address before deploying.
NDBC_CONTACT = os.environ.get('NDBC_CONTACT', 'unset-contact@example.com')
USER_AGENT = f'ndbc-buoy-scraper/1.0 (+{NDBC_CONTACT})'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# ---------------------------------------------------------------------------
# Concurrency and throttling.
#
# The previous config paired CONCURRENT_REQUESTS_PER_DOMAIN = 1 with
# DOWNLOAD_DELAY = 0, i.e. strictly sequential but issued as fast as NDBC could
# answer, with no backoff when it slowed down. AutoThrottle replaces that with
# a latency-derived delay: when NDBC gets slower, the crawler backs off on its
# own, which is the behaviour that keeps a 17,050-request crawl welcome.
#
# Everything targets one domain, so CONCURRENT_REQUESTS above the per-domain
# cap buys nothing -- they are kept equal to say so plainly.
# ---------------------------------------------------------------------------
CONCURRENT_REQUESTS = 2
CONCURRENT_REQUESTS_PER_DOMAIN = 2
DOWNLOAD_DELAY = 0.25

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 30.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False

# A multi-hour crawl will meet transient 5xx and connection resets; without
# retries those buoy-years are silently missing from bronze until someone
# notices a gap in the data.
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]
DOWNLOAD_TIMEOUT = 180

# ---------------------------------------------------------------------------
# Memory ceiling. The deployment target has 6 GB shared with Postgres, and
# each response is fully materialized (read_csv -> DataFrame -> dicts) before
# the pipeline writes it. If the crawler is going to die on memory, it should
# die on its own terms -- logging what happened and closing cleanly -- rather
# than being reaped by the kernel OOM killer, which leaves no diagnosis and a
# stale 'running' row in pipeline_runs.
# ---------------------------------------------------------------------------
MEMUSAGE_ENABLED = True
MEMUSAGE_LIMIT_MB = int(os.environ.get('SCRAPY_MEMUSAGE_LIMIT_MB', '1200'))
MEMUSAGE_WARNING_MB = int(os.environ.get('SCRAPY_MEMUSAGE_WARNING_MB', '900'))

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'ndbc_buoy_scraper.pipelines.BronzeParquetPipeline': 300,
}

# Records each crawl into pipeline_runs alongside the silver stage, so both
# halves of the pipeline are observable from the same SQL.
EXTENSIONS = {
    'ndbc_buoy_scraper.extensions.PipelineRunTracker': 500,
}
# Set to '0' to run the scrape without a reachable Postgres (the tracker
# degrades to a no-op either way, but this skips the connection attempt).
TRACK_RUNS = os.environ.get('TRACK_RUNS', '1') not in {'0', 'false', 'no'}

# Root directory for the bronze landing zone (Hive-partitioned by
# buoy_id/year). Overridable via env so Docker/DuckDB can mount the same
# path as a shared volume without code changes.
BRONZE_ROOT = os.environ.get('BRONZE_ROOT', 'data/bronze')

# Skip re-downloading buoy-years already in bronze whose year has closed.
# NDBC's historical year files are immutable once the year ends, so a full
# re-crawl re-fetches ~17,000 files to discover that ~1,350 changed. Set
# NDBC_FULL_REFRESH=1 to force a complete re-crawl (e.g. after changing the
# bronze decode).
NDBC_FULL_REFRESH = os.environ.get('NDBC_FULL_REFRESH', '') in {'1', 'true', 'yes'}

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"
