from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, lower, split, trim, upper, when
from pyspark.sql.types import StringType

from src.helu.utils.silver_core.silver_data_process import SilverPipeline


class SilverApfelPipeline(SilverPipeline):
    def get_valid_data(self, df: DataFrame) -> DataFrame:
        logger.info("Getting valid data")
        df_check_ts_data = df.withColumn(
            "is_timestamp_valid", (col("signup_timestamp") <= col("event_timestamp"))
        )
        df_ts_invalid = df_check_ts_data.where(~col("is_timestamp_valid"))
        df_ts_valid = df_check_ts_data.where(col("is_timestamp_valid")).drop(
            "is_timestamp_valid"
        )

        if df_ts_invalid and not df_ts_invalid.isEmpty():
            logger.info(
                "Audit invalid data where sihgnup_timestamp is after event_timestamp"
            )
            df_ts_invalid = df_ts_invalid.withColumn(
                "_invalid_reason", lit("invalid_timestamp_data")
            )
            self.writer.append_data(
                df=df_ts_invalid, custom_table_name="apfel_invalid_data"
            )

        for field in df_ts_valid.schema.fields:
            if isinstance(field.dataType, StringType) and field.name in [
                "event_type",
                "processing_status",
            ]:
                df_ts_valid = df_ts_valid.withColumn(
                    field.name, lower(trim(col(field.name)))
                )

        valid_financials = (
            (col("price_amount") >= 0)
            & (col("tax_amount") >= 0)
            & col("currency").isNotNull()
        )

        valid_sub_status = [
            "subscription_renewed",
            "subscription_started",
            "subscription_cancelled",
        ]

        valid_business = (
            lower(col("event_type")).isin(valid_sub_status)
            & col("subscription_plan").isNotNull()
            & col("country_code").isNotNull()
            & (trim(col("country_code")) != "")
        )

        all_valid = valid_financials & valid_business

        df_silver_clean = df_ts_valid.where(all_valid)
        df_silver_dirty = df_ts_valid.where(~all_valid)

        if df_silver_dirty and not df_silver_dirty.isEmpty():
            logger.info("Auditing invalid business data")
            df_silver_dirty = df_silver_dirty.withColumn(
                "_invalid_reason", lit("invalid_business_data")
            )
            self.writer.append_data(
                df=df_silver_dirty, custom_table_name="apfel_invalid_data"
            )

        return df_silver_clean

    def get_columns_enriched(self, df: DataFrame) -> DataFrame:
        logger.info("Enriching data")
        enriched_df = df.withColumn(
            "subscription_status", split(col("event_type"), "_").getItem(1)
        )

        enriched_df = enriched_df.withColumn(
            "currency_norm", upper(trim(col("currency")))
        )

        enriched_df = enriched_df.withColumn(
            "currency_norm",
            when(col("currency_norm").isin("EUR", "EURO"), "EUR")
            .when(col("currency_norm") == "USD", "USD")
            .when(col("currency_norm") == "GBP", "GBP")
            .otherwise(col("currency_norm")),
        )

        return enriched_df

    def move_to_silver(self):
        logger.info("Starting")
        bronze_df = self.read_bronze()
        if bronze_df and not bronze_df.isEmpty():
            bronze_df = self.writer.add_metadata_columns(df=bronze_df, layer="silver")

            renamed_df = self.rename_columns(df=bronze_df)
            casted_df = self.cast_timestamps_data(df=renamed_df)
            clean_df = self.remove_invalid_data(df=casted_df)
            deduplicated_df = self.remove_duplicates(df=clean_df)

            valid_data_df = self.get_valid_data(df=deduplicated_df)
            enriched_df = self.get_columns_enriched(df=valid_data_df)

            self.writer.create_or_upsert(df=enriched_df)
        else:
            logger.info("No data to process")
