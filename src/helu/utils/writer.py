from pyspark.sql.functions import lit, current_timestamp, to_date, to_utc_timestamp
from pyspark.sql import SparkSession, DataFrame
from loguru import logger
from delta.tables import DeltaTable


from src.helu.utils.job_parameters import JobParameters

class Writer:
    def __init__(self, spark: SparkSession, job_parameters: JobParameters):
        self.spark = spark
        self.source = job_parameters.source
        self.destination_path = job_parameters.destination_path
        self.job_parameters = job_parameters
        self.final_table_path = f"{self.destination_path}/{self.source}"

    def add_metadata_columns(self, df: DataFrame, layer: str = None) -> DataFrame:
        if layer is None:
            return (
            df
            .withColumn("_ingestion_timestamp_utc", to_utc_timestamp(current_timestamp(), "UTC"))
            .withColumn("_source", lit(self.source))
            .withColumn("_inserted_date_utc", to_date(to_utc_timestamp(current_timestamp(), "UTC")))
            )

        return (
            df
            .withColumn(f"_{layer}_ingestion_timestamp_utc", to_utc_timestamp(current_timestamp(), "UTC"))
            .withColumn("_inserted_date_utc", to_date(to_utc_timestamp(current_timestamp(), "UTC")))
            )

    def append_data(self, df: DataFrame, custom_table_name:str = None) -> str:
        append_path = self.final_table_path
        logger.info(f"Appending Delta table at {append_path}")

        if custom_table_name:
            append_path = f"{self.destination_path}/{custom_table_name}"

        writer = df.write.format("delta").option("mergeSchema", "true").mode("append")

        if self.job_parameters.partitioned_columns:
            writer = writer.partitionBy(self.job_parameters.partitioned_columns)

        writer.save(append_path)

        logger.info(f"Finished appending to raw")

    def get_update_schema(self, df: DataFrame):
        columns = df.columns
        should_not_updated_columns = ["_ingestion_timestamp_utc"]
        update_scheme = {i: f"s.{i}" for i in columns if i not in should_not_updated_columns}  # Deprecated column kitchen_ingestion_timestamp
        return update_scheme

    def upsert_data(self, df_new: DataFrame, update_schema=None):
        logger.info("starting upsert")

        if update_schema is None:
            update_schema = self.get_update_schema(df_new)

        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        delta_df = DeltaTable.forPath(self.spark, self.final_table_path)

        if self.job_parameters.update_condition:
            delta_df.alias("t").merge(df_new.alias("s"), self.job_parameters.merge_logic).whenMatchedUpdate(  # type: ignore
                condition=self.job_parameters.update_condition,
                set=update_schema,  # type: ignore
            ).whenNotMatchedInsertAll().execute()
        else:
            delta_df.alias("t").merge(df_new.alias("s"), self.job_parameters.merge_logic).whenMatchedUpdate(  # type: ignore
                set=update_schema,  # type: ignore
            ).whenNotMatchedInsertAll().execute()

    def create_table(self, df: DataFrame):
        logger.info("starting creating table")
        writer = (
            df.write.format("delta").option("mergeSchema", "true").mode("overwrite")
        )
        if self.job_parameters.partitioned_columns:
            writer = writer.partitionBy(self.job_parameters.partitioned_columns)
        writer.save(self.final_table_path)
        logger.info(f"Finished creating table at {self.final_table_path}")

    def create_or_upsert(self, df: DataFrame):
        if DeltaTable.isDeltaTable(self.spark, self.final_table_path):
            logger.info(f"Delta table already exists at {self.final_table_path}. Starting upsert.")
            self.upsert_data(df_new=df)
            logger.info(f"Finished upsert to Delta table at {self.final_table_path}")
        else:
            logger.info(f"Creating Delta table at {self.final_table_path}")
            self.create_table(df=df)
            logger.info(f"Finished creating Delta table at {self.final_table_path}")