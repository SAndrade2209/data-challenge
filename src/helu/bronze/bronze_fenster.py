from src.helu.utils.bronze_core.bronze_data_processor import BronzeDataProcessor
from src.helu.utils.job_parameters import JobParameters
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession

def bronze_fenster_job():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("BronzeFenster")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    fenster_landing_schema = StructType([
        StructField("id", StringType()),
        StructField("ts", StringType()),
        StructField("type", StringType()),
        StructField("cid", StringType()),
        StructField("mail", StringType()),
        StructField("signup_ts", StringType()),
        StructField("ctry", StringType()),
        StructField("state", StringType()),
        StructField("zip", StringType()),
        StructField("plan", StringType()),
        StructField("ccy", StringType()),
        StructField("price", DoubleType()),
        StructField("tax", DoubleType()),
        StructField("vat_id", StringType()),
        StructField("campaign_src", StringType()),
        StructField("utm_medium", StringType()),
        StructField("utm_campaign", StringType()),
        StructField("browser", StringType()),
        StructField("os", StringType()),
        StructField("screen_res", StringType()),
        StructField("lang", StringType()),
        StructField("tz", StringType()),
        StructField("legacy_flag", StringType()),
        StructField("migrated_from", StringType()),
        StructField("batch_id", StringType()),
        StructField("row_hash", StringType()),
    ])


    job_parameters = JobParameters(
        source="fenster",
        destination_schema="bronze",
        landing_schema=fenster_landing_schema,
        partitioned_columns=["_inserted_date_utc"]
    )
    processor = BronzeDataProcessor(
        spark=spark,
        job_parameters=job_parameters,
        source_format="csv",
    )

    processor.move_to_bronze()

if __name__ == "__main__":
    bronze_fenster_job()
