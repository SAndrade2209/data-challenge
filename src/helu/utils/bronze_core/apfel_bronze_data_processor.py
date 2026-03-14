from src.helu.utils.bronze_core.bronze_data_processor import BronzeDataProcessor
from loguru import logger
from pyspark.sql.functions import explode


class ApfelBronzeDataProcessor(BronzeDataProcessor):
    def move_to_bronze(self):
        logger.info(f"Starting bronze ingestion for '{self.source}'")
        df = self.read_from_landing()
        if df and not df.isEmpty():
            logger.info(f"Writing bronze metadata")
            bronze_df = df.withColumn("event", explode("events")).select("event.*")
            final_df = self.writer.add_metadata_columns(bronze_df)
            self.writer.append_data(final_df)
            self.archive_to_processed(df)
        else:
            logger.warning(f"No data to process for '{self.source}'")
        logger.info(f"Bronze ingestion complete for '{self.source}'")

