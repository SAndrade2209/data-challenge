from src.helu.utils.bronze_core.apfel_bronze_data_processor import ApfelBronzeDataProcessor
from src.helu.utils.job_parameters import JobParameters
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, LongType
from pyspark.sql import SparkSession


def bronze_apfel_job():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("BronzeApfel")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("event_type", StringType()),
        StructField("customer_uuid", StringType()),
        StructField("customer_email", StringType()),
        StructField("customer_created_at", StringType()),
        StructField("country_code", StringType()),
        StructField("region", StringType()),
        StructField("city", StringType()),
        StructField("postal_code", StringType()),
        StructField("subscription_type", StringType()),
        StructField("renewal_period", StringType()),
        StructField("currency", StringType()),
        StructField("amount", DoubleType()),
        StructField("tax_amount", DoubleType()),
        StructField("discount_code", StringType()),
        StructField("affiliate_id", StringType()),
        StructField("device_type", StringType()),
        StructField("app_version", StringType()),
        StructField("session_id", StringType()),
        StructField("internal_ref", StringType()),
        StructField("processing_status", StringType()),
    ])

    # JSON from API/fallback: {"events": [...], "count": N}
    landing_schema = StructType([
        StructField("events", ArrayType(event_schema)),
        StructField("count", LongType()),
    ])

    job_parameters = JobParameters(
        source="apfel",
        destination_schema="bronze",
        landing_schema=landing_schema,
        partitioned_columns=["_inserted_date_utc"]
    )
    processor = ApfelBronzeDataProcessor(
        spark=spark,
        job_parameters=job_parameters,
        source_format="json",
    )

    processor.move_to_bronze()

if __name__ == "__main__":
    bronze_apfel_job()
