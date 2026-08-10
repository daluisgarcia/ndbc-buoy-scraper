import io
from collections.abc import Generator

import pandas as pd
import scrapy
from scrapy.http import Response

from ndbc_buoy_scraper.coordinates_converter import (
    convert_coordinates,
    is_valid_coordinates,
)


class NDBCStandardMeterologicalSpider(scrapy.Spider):
    name = 'ndbc_standard_meterological'
    start_urls = ['https://www.ndbc.noaa.gov/historical_data.shtml']
    allowed_domains = ['www.ndbc.noaa.gov']
    _urls_seen = [] # List of urls already seen to avoid going to the same page again

    def _get_data_from_url(self, response: Response, buoy_id: str) -> Generator[dict]:
        if response.url in self._urls_seen:
            print(f"URL {response.url} already seen, skipping.")
            yield {}

        df = pd.read_csv(io.StringIO(response.text), sep=r'\s+', low_memory=False) # type: ignore

        # Drop the first row which contains the units of the columns
        df.drop(index=0, inplace=True)

        year_columnm_name = '#YY' if '#YY' in df.columns else 'YYYY' if 'YYYY' in df.columns else 'YY'

        if year_columnm_name == 'YY':  # Avoid ambiguity if both 'YY' and 'YYYY' are present, prioritize 'YYYY'
            df[year_columnm_name] = df[year_columnm_name].apply(lambda x: f"20{x}" if int(x) < 50 else f"19{x}")

        df.rename(columns={
            year_columnm_name: 'year',
            'MM': 'month',
            'DD': 'day',
            'hh': 'hour',
            'mm': 'minute',
            'WDIR': 'wind_direction',
            'WSPD': 'wind_speed',
            'GST': 'gust',
            'WVHT': 'significant_wave_height',
            'DPD': 'dominant_wave_period',
            'APD': 'average_wave_period',
            'MWD': 'mean_wave_direction',
            'PRES': 'pressure',
            'ATMP': 'air_temperature',
            'WTMP': 'sea_surface_temperature',
            'DEWP': 'dew_point_temperature',
            'VIS': 'visibility',
            'PTDY': 'pressure_tendency',
            'TIDE': 'tide',
        }, inplace=True)
        df['buoy_id'] = buoy_id

        df['datetime'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
        df.drop(columns=['year', 'month', 'day', 'hour'], inplace=True)
        if 'minute' in df.columns:
            df.drop(columns=['minute'], inplace=True)

        self._urls_seen.append(response.url) # Add the URL to the list of seen URLs

        yield from df.to_dict(orient='records')

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
                    'buoy_id': buoy_id,
                    'latitude': latitude,
                    'longitude': longitude
                }
            else:
                print(f"Coordinates not found for buoy {buoy_id} in the station summary page.")
                yield {
                    'buoy_id': buoy_id,
                    'latitude': None,
                    'longitude': None
                }
        except ValueError as e:
            print(f"Error converting coordinates for buoy {buoy_id}: {e}")
            yield {
                'buoy_id': buoy_id,
                'latitude': None,
                'longitude': None
            }

    def parse(self, response: Response) -> Generator:
        buoys_id_and_data_years = response.xpath('//a[@id="stdmet"]/../ul[@class="histfiles"]/li')

        print(f"Found {len(buoys_id_and_data_years)} buoys with historical data links.")

        for buoys in buoys_id_and_data_years:
            buoy_id = buoys.xpath('./text()[1]').get()
            if buoy_id is None:
                raise ValueError("Buoy ID not found in the HTML structure.")

            buoy_id = buoy_id.strip().replace(':', '')

            data_years_links = buoys.xpath('./a/@href').getall()

            for data_year_link in data_years_links:
                # Get the measurement data for the buoy and year
                txt_data_link = data_year_link.replace('download_data', 'view_text_file')
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


if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess

    process = CrawlerProcess()
    process.crawl(NDBCStandardMeterologicalSpider)
    process.start()
