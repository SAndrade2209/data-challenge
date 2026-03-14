from src.helu.utils.job_parameters import JobParameters
from src.helu.utils.silver_core.silver_data_process import SilverPipeline
from pyspark.sql import SparkSession

def silver_exchange_job():
    spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("SilverExchangeRate")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    job_parameters = JobParameters(
        source="exchange_rates",
        origin_schema="bronze",
        destination_schema="silver",
        is_dimension=True,
        partitioned_columns=["_inserted_date_utc"],

    )

    silver_pipeline = SilverPipeline(spark, job_parameters)
    silver_pipeline.move_to_silver()

if __name__ == "__main__":
    silver_exchange_job()
