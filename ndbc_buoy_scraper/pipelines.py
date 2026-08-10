# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter
import pandas as pd


class NdbcScraperPipeline:
    def process_item(self, item, spider):
        return item


class BatchProcessingPipeline:
    def __init__(self, batch_size=100):
        self.buoy_data_saving_path = 'batch_output.csv'

        if pd.io.common.file_exists(self.buoy_data_saving_path): # type: ignore
            raise FileExistsError(f"The file {self.buoy_data_saving_path} already exists. Please remove it before running the spider to avoid data loss.")
        
        self.buoy_coordinates_saving_path = 'buoy_coordinates.csv'

        if pd.io.common.file_exists(self.buoy_coordinates_saving_path): # type: ignore
            raise FileExistsError(f"The file {self.buoy_coordinates_saving_path} already exists. Please remove it before running the spider to avoid data loss.")

        self.batch_size = batch_size
        self.buoy_data_items = []
        self.buoy_coordinates_df = pd.DataFrame(columns=['buoy_id', 'latitude', 'longitude'])

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            batch_size=crawler.settings.getint('BATCH_SIZE', 100)
        )

    def process_item(self, item: dict):
        if 'latitude' not in item:
            self.buoy_data_items.append(dict(item))
            
            if len(self.buoy_data_items) >= self.batch_size:
                self._process_batch()
        else:
            self.buoy_coordinates_df = pd.concat(
                [self.buoy_coordinates_df, pd.DataFrame([dict(item)])],
                ignore_index=True
            )

        return item

    def _process_batch(self):
        if not self.buoy_data_items:
            return

        df_batch = pd.DataFrame(self.buoy_data_items)

        df_batch.to_csv(
            self.buoy_data_saving_path,
            mode='a',
            index=False,
            header=not pd.io.common.file_exists(self.buoy_data_saving_path) # type: ignore
        )

        self.buoy_data_items = []

    def close_spider(self):
        if self.buoy_data_items:
            self._process_batch()

        self.buoy_coordinates_df.to_csv(self.buoy_coordinates_saving_path, index=False)
