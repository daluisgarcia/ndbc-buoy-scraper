import datetime as dt
import hashlib
import io
import os
import re
from collections.abc import Generator
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pyarrow.parquet as pq
import scrapy
from scrapy.http import Response

from ndbc_buoy_scraper.coordinates_converter import (
    convert_coordinates,
    is_valid_coordinates,
)

# NDBC stdmet historical filenames follow "<buoy_id>h<YYYY>.txt.gz", e.g.
# "41001h2020.txt.gz", verified against a live view_text_file.php response.
FILENAME_YEAR_PATTERN = re.compile(r'h(\d{4})\.txt')


def source_identity(url: str) -> tuple[str, str]:
    """Derive (bronze source stem, four-digit year) from an NDBC data URL.

    Shared by `parse` -- which needs it BEFORE issuing a request, to decide
    whether that request can be skipped -- and by the download callback, which
    needs it to name the bronze partition. Both must agree exactly: a mismatch
    would make the skip check consult a path the writer never uses, silently
    disabling incremental crawling.
    """
    filename = parse_qs(urlparse(url).query).get('filename', [''])[0]
    if filename:
        source_stem = re.sub(r'\.txt\.gz$', '', filename)
    else:
        # No filename param: fall back to a stable per-URL hash so distinct
        # malformed responses land in distinct files instead of silently
        # overwriting the same bronze partition.
        source_stem = f'unknown-{hashlib.sha1(url.encode()).hexdigest()[:12]}'

    year_match = FILENAME_YEAR_PATTERN.search(filename)
    year = year_match.group(1) if year_match else 'unknown'
    return source_stem, year


class NDBCStandardMeterologicalSpider(scrapy.Spider):
    name = 'ndbc_standard_meterological'
    start_urls = ['https://www.ndbc.noaa.gov/historical_data.shtml']
    allowed_domains = ['www.ndbc.noaa.gov']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Instance-level, and a set. As a mutable CLASS attribute this leaked
        # across every spider instance in a process (so the second crawl in a
        # CrawlerProcess would skip everything the first one fetched), and as a
        # list its membership test was O(n) -- quadratic over the 17,050 URLs a
        # full NDBC crawl visits.
        self._urls_seen: set[str] = set()
        self._skipped_cached = 0

    def _bronze_path(self, buoy_id: str, year: str, source_stem: str) -> str:
        return os.path.join(
            self.settings.get('BRONZE_ROOT', 'data/bronze'),
            'observations',
            f'buoy_id={buoy_id}',
            f'year={year}',
            f'{source_stem}.parquet',
        )

    def _bronze_file_is_usable(self, path: str) -> bool:
        """True when `path` holds a Parquet file the silver stage can read.

        Existence alone is the wrong test. A zero-byte or zero-column Parquet --
        left behind by a killed run, a full disk, or the schema-loss bug that
        used to drop the header of a row-less NDBC file -- makes DuckDB reject
        the read outright ("Need at least one non-root column in the file"),
        failing the ENTIRE year rather than just this buoy.

        Counting such a file as "already have" is what makes that fatal: a
        closed year is never re-fetched, so nothing would ever replace the bad
        file and every later silver run would die in the same place. Reading the
        footer instead lets the next crawl overwrite it -- the pipeline
        self-heals rather than wedging.

        One footer read (~40us) per cached file: ~0.6s across a full
        15,700-file bronze tree, against the ~17,000 HTTP requests the skip
        avoids.
        """
        try:
            return pq.ParquetFile(path).metadata.num_columns > 0
        except Exception:
            # Deliberately broad. Absent, truncated, zero-byte, and
            # not-actually-Parquet all mean the same thing to the caller:
            # this buoy-year is not safely in bronze, so re-fetch it.
            return False

    def _already_have(self, buoy_id: str, year: str, source_stem: str) -> bool:
        """True when this buoy-year is READABLE in bronze AND can never change.

        NDBC rewrites only the in-progress year's file; once a year closes its
        historical file is immutable. Re-fetching those is the difference
        between ~17,000 requests per run and ~1,350.
        """
        if self.settings.getbool('NDBC_FULL_REFRESH', False):
            return False
        # UTC, not local time. NDBC timestamps are UTC and its year files roll
        # over on the UTC boundary; a host west of Greenwich would otherwise
        # spend the last hours of December 31st treating the incoming year as
        # already closed and skipping its file.
        current_year = dt.datetime.now(dt.UTC).year
        if not year.isdigit() or int(year) >= current_year:
            return False
        return self._bronze_file_is_usable(
            self._bronze_path(buoy_id, year, source_stem)
        )

    def _get_data_from_url(self, response: Response, buoy_id: str) -> Generator[dict]:
        if response.url in self._urls_seen:
            self.logger.debug(f"URL {response.url} already seen, skipping.")
            return

        # Mechanical decode only: no unit-row drop, no renames, no datetime
        # construction. All semantic transforms live in
        # ndbc_buoy_scraper/silver.py (DuckDB).
        df = pd.read_csv(io.StringIO(response.text), sep=r'\s+', low_memory=False) # type: ignore

        source_stem, year = source_identity(response.url)

        self._urls_seen.add(response.url)

        yield {
            'kind': 'observations',
            'buoy_id': buoy_id,
            'year': year,
            'source_stem': source_stem,
            # Carried separately because `to_dict(orient='records')` erases the
            # schema of a row-less frame: 18 column names go in, a bare `[]`
            # comes out. NDBC serves header-only files for buoy-years with no
            # reports (e.g. 47072h2003), and rebuilding one from `records`
            # alone yields a zero-column Parquet that DuckDB cannot read at
            # all -- taking the whole year's load down with it. The writer uses
            # this to keep the header. See BronzeParquetPipeline.
            'columns': list(df.columns),
            'records': df.to_dict(orient='records'),
        }

    def _get_station_summary(self, response: Response, buoy_id: str) -> Generator[dict]:
        # This function is a placeholder for future implementation to extract station summary data
        # from the station summary page. For now, it just yields an empty dictionary.
        coordinates_text = response.xpath('//div[@id="stn_metadata"]/p[1]/b[4]/text()').get()

        if not is_valid_coordinates(coordinates_text):
            coordinates_text = response.xpath('//div[@id="stn_metadata"]/p[1]/b[3]/text()').get()

        if not is_valid_coordinates(coordinates_text):
            coordinates_text = response.xpath('//div[@id="stn_metadata"]/p[1]/b[2]/text()').get()

        if not is_valid_coordinates(coordinates_text):
            coordinates_text = response.xpath('//div[@id="stn_metadata"]/p[1]/b[5]/text()').get()

        try:
            if coordinates_text:
                latitude, longitude = convert_coordinates(coordinates_text)
                yield {
                    'kind': 'coordinates',
                    'buoy_id': buoy_id,
                    'latitude': latitude,
                    'longitude': longitude
                }
            else:
                self.logger.warning(
                    f"Coordinates not found for buoy {buoy_id} in the station summary page."
                )
                self.crawler.stats.inc_value('ndbc/coordinates_missing')
                yield {
                    'kind': 'coordinates',
                    'buoy_id': buoy_id,
                    'latitude': None,
                    'longitude': None
                }
        except ValueError as e:
            self.logger.warning(f"Error converting coordinates for buoy {buoy_id}: {e}")
            self.crawler.stats.inc_value('ndbc/coordinates_unparseable')
            yield {
                'kind': 'coordinates',
                'buoy_id': buoy_id,
                'latitude': None,
                'longitude': None
            }

    def parse(self, response: Response) -> Generator:
        buoys_id_and_data_years = response.xpath('//a[@id="stdmet"]/../ul[@class="histfiles"]/li')

        if not buoys_id_and_data_years:
            # An empty result here means the page structure moved under us. The
            # crawl would otherwise "succeed" with zero items and the silver
            # stage would happily reload the unchanged bronze, so nothing would
            # look broken until someone noticed the data had stopped advancing.
            raise ValueError(
                'no station entries matched the stdmet histfiles XPath -- '
                'NDBC likely changed the page structure'
            )

        self.logger.info(
            f'Found {len(buoys_id_and_data_years)} buoys with historical data links.'
        )

        for buoys in buoys_id_and_data_years:
            buoy_id = buoys.xpath('./text()[1]').get()
            if buoy_id is None:
                raise ValueError("Buoy ID not found in the HTML structure.")

            buoy_id = buoy_id.strip().replace(':', '')

            data_years_links = buoys.xpath('./a/@href').getall()

            for data_year_link in data_years_links:
                # Get the measurement data for the buoy and year
                txt_data_link = data_year_link.replace('download_data', 'view_text_file')

                source_stem, year = source_identity(txt_data_link)
                if self._already_have(buoy_id, year, source_stem):
                    self._skipped_cached += 1
                    continue

                yield response.follow(
                    txt_data_link,
                    callback=self._get_data_from_url,
                    cb_kwargs={'buoy_id': buoy_id}
                )

            # Get the station summary page for the buoy to extract coordinates and other metadata
            station_summary_link = f'https://www.ndbc.noaa.gov/station_page.php?station={buoy_id}'
            yield response.follow(
                station_summary_link,
                callback=self._get_station_summary,
                cb_kwargs={'buoy_id': buoy_id}
            )

    def closed(self, reason):
        # Surfaced through Scrapy's stats dict, which PipelineRunTracker
        # persists into pipeline_runs.scrapy_stats -- so "how much did
        # incremental crawling actually save?" is answerable in SQL.
        self.crawler.stats.set_value('ndbc/skipped_cached', self._skipped_cached)


if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess

    process = CrawlerProcess()
    process.crawl(NDBCStandardMeterologicalSpider)
    process.start()
