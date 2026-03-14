import glob
import os
from datetime import datetime, timezone
from typing import Union

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from src.helu.utils.definitions_enums import SourceFormat
from src.helu.utils.job_parameters import JobParameters
from src.helu.utils.writer import Writer


class BronzeDataProcessor:
    def __init__(
        self,
        spark: SparkSession,
        job_parameters: JobParameters,
        source_format: Union[str, SourceFormat],
        writer: Writer = None,
    ):
        self.spark = spark
        self.source = job_parameters.source
        self.source_format = (
            source_format
            if isinstance(source_format, SourceFormat)
            else SourceFormat.from_string(source_format)
        )
        self.landing_schema = job_parameters.landing_schema
        self.landing_path = job_parameters.landing_path
        self.writer = writer if writer else Writer(spark, job_parameters)
        self.job_parameters = job_parameters

    def read_from_landing(self) -> DataFrame:
        landing_glob = os.path.join(
            self.landing_path, "**", f"*.{self.source_format.value}"
        )
        files = glob.glob(landing_glob, recursive=True)
        error_path = self.landing_path.replace("inbound", "error")

        if not files:
            logger.warning(
                f"No {self.source_format.value} files found in {self.landing_path}"
            )
            return None

        logger.info(f"Found {len(files)} file(s) in {self.landing_path}")

        if self.landing_schema:
            df = (
                self.spark.read.format(self.source_format.value)
                .schema(self.landing_schema)
                .options(header="true")
                .option("badRecordsPath", error_path)
                .load(files)
            )
        else:
            df = (
                self.spark.read.format(self.source_format.value)
                .options(header="true", inferSchema="true")
                .option("badRecordsPath", error_path)
                .load(files)
            )

        logger.info(f"Read {df.count()} rows from landing for source '{self.source}'")
        return df

    def archive_to_processed(self, df: DataFrame) -> None:
        processed_base = self.landing_path.replace("inbound", "processed")
        current_date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        processed_path = os.path.join(processed_base, current_date_utc)

        os.makedirs(processed_path, exist_ok=True)

        df.write.mode("overwrite").parquet(processed_path)
        logger.info(f"Archived to processed: {processed_path}")

        landing_glob = os.path.join(
            self.landing_path, "**", f"*.{self.source_format.value}"
        )
        files = glob.glob(landing_glob, recursive=True)
        for f in files:
            os.remove(f)
            logger.info(f"Removed from inbound: {f}")

    def move_to_bronze(self):
        logger.info(f"Starting bronze ingestion for '{self.source}'")
        df = self.read_from_landing()
        if df and not df.isEmpty():
            logger.info("Writing bronze metadata")
            df = self.writer.add_metadata_columns(df)
            self.writer.append_data(df)
            self.archive_to_processed(df)
        else:
            logger.warning(f"No data to process for '{self.source}'")
        logger.info(f"Bronze ingestion complete for '{self.source}'")
