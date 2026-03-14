from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number, to_timestamp
from pyspark.sql.window import Window

from src.helu.utils.job_parameters import JobParameters
from src.helu.utils.transversal_methods import PipelineFunctions
from src.helu.utils.writer import Writer


class SilverPipeline:
    def __init__(
        self, spark: SparkSession, job_parameters: JobParameters, writer: Writer = None
    ):
        self.job_parameters = job_parameters
        self.spark = spark
        self.writer = writer if writer else Writer(spark, job_parameters)
        self.destination_path = job_parameters.destination_path
        self.origin_path = job_parameters.origin_path
        self.destination_table_path = (
            f"{self.destination_path}/{self.job_parameters.source}"
        )
        self.origin_table_path = f"{self.origin_path}/{self.job_parameters.source}"

    def read_bronze(self, filter_date: str = None) -> DataFrame:
        logger.info(f"Reading bronze table {self.origin_table_path}")
        df = self.spark.read.format("delta").load(self.origin_table_path)
        filter_date = filter_date if filter_date else "_inserted_date_utc"

        if DeltaTable.isDeltaTable(self.spark, self.destination_table_path):
            logger.info(f"Reading silver table {self.destination_table_path}")
            silver_df = self.spark.read.format("delta").load(
                self.destination_table_path
            )
            last_processing_date = PipelineFunctions.get_last_processing_date(
                silver_df, filter_date
            )

            if last_processing_date:
                logger.info(
                    f"Last processing date found: {last_processing_date}. Reading new data from bronze."
                )
                bronze_df = df.where(f"{filter_date} >= '{last_processing_date}'")
                return bronze_df
            else:
                logger.info(
                    "No last processing date found. Reading all data from bronze."
                )
                return df
        else:
            logger.info("Silver table does not exist. Reading all data from bronze.")
            return df

    def rename_columns(self, df: DataFrame) -> DataFrame:
        logger.info("Renaming columns")
        if self.job_parameters.columns_to_rename:
            return df.withColumnsRenamed(self.job_parameters.columns_to_rename)
        else:
            return df

    def remove_invalid_data(self, df: DataFrame) -> DataFrame:
        logger.info("Removing invalid data")
        is_valid = (
            col("customer_id").isNotNull()
            & col("email").isNotNull()
            & col("signup_timestamp").isNotNull()
            & col("event_timestamp").isNotNull()
        )

        df_clean = df.where(is_valid)
        df_invalid = df.where(~is_valid)

        if df_invalid and not df_invalid.isEmpty():
            logger.info("Saving invalid data")
            df_invalid = df_invalid.withColumn(
                "_invalid_reason", lit("id_data_is_null")
            )

            self.writer.append_data(
                df=df_invalid,
                custom_table_name=f"{self.job_parameters.source}_invalid_data",
            )

        return df_clean

    def cast_timestamps_data(self, df: DataFrame) -> DataFrame:
        logger.info(f"Cast timestamps for {self.job_parameters.source}")
        return df.withColumn(
            "event_timestamp", to_timestamp(col("event_timestamp"))
        ).withColumn("signup_timestamp", to_timestamp(col("signup_timestamp")))

    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        logger.info("Removing duplicate data")
        df_row = df.withColumn(
            "row_number",
            row_number().over(
                Window.partitionBy(self.job_parameters.identifier).orderBy(
                    col(self.job_parameters.deduplication_sorting_column).desc()
                )
            ),
        )
        df_deduplicated = df_row.where("row_number = 1").drop(df_row.row_number)
        logger.info("Finished removing duplicates")
        return df_deduplicated

    def move_to_silver(self, filter_date: str = None):
        logger.info("Starting")
        bronze_df = self.read_bronze()
        if bronze_df and not bronze_df.isEmpty():
            bronze_df = self.writer.add_metadata_columns(df=bronze_df, layer="silver")

            if self.job_parameters.is_dimension:
                logger.info("Moving dimension table")
                subset_cols = [c for c in bronze_df.columns if not c.startswith("_")]
                bronze_df = bronze_df.drop_duplicates(subset=subset_cols)

                self.writer.create_table(df=bronze_df)

            else:
                logger.info("Moving fact table")
                final_df = self.rename_columns(df=bronze_df)
                cast_df = self.cast_timestamps_data(df=final_df)
                valid_df = self.remove_invalid_data(df=cast_df)

                deduplicated_df = self.remove_duplicates(df=valid_df)

                self.writer.create_or_upsert(df=deduplicated_df)

        else:
            logger.info("Bronze dataframe is empty. No data to move to silver.")
