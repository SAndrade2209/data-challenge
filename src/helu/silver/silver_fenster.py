from src.helu.utils.job_parameters import JobParameters
from src.helu.utils.silver_core.silver_fenster_pipeline import SilverFensterPipeline
from pyspark.sql import SparkSession

def silver_fenster_job():
    spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("SilverFenster")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    job_parameters = JobParameters(
        source="fenster",
        origin_schema="bronze",
        destination_schema="silver",
        merge_logic="s.customer_id = t.customer_id",
        update_condition="s.customer_id = t.customer_id",
        identifier="customer_id",
        deduplication_sorting_column="event_timestamp",
        partitioned_columns=["_inserted_date_utc"],
        columns_to_rename={
            "id": "event_id",
            "ts": "event_timestamp",
            "type": "subscription_status",
            "cid": "customer_id",
            "mail": "email",
            "signup_ts": "signup_timestamp",
            "ctry": "country",
            "zip": "zip_code",
            "plan": "original_subscription_plan",
            "ccy": "currency",
            "price": "price_amount",
            "tax": "tax_amount",
            "campaign_src": "campaign_source",
            "os": "operating_system",
            "screen_res": "screen_resolution",
            "lang": "language",
            "tz": "time_zone",
        },
    )

    silver_pipeline = SilverFensterPipeline(spark, job_parameters)
    silver_pipeline.move_to_silver()

if __name__ == "__main__":
    silver_fenster_job()
