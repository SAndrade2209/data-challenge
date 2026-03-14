from pyspark.sql import SparkSession

from src.helu.utils.bronze_core.bronze_data_processor import BronzeDataProcessor
from src.helu.utils.job_parameters import JobParameters


def bronze_exchange_job():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("BronzeExchangeRate")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    job_parameters = JobParameters(
        source="exchange_rates",
        destination_schema="bronze",
        partitioned_columns=["_inserted_date_utc"],
    )
    processor = BronzeDataProcessor(
        spark=spark,
        job_parameters=job_parameters,
        source_format="csv",
    )

    processor.move_to_bronze()


if __name__ == "__main__":
    bronze_exchange_job()
