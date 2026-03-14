from src.helu.utils.job_parameters import JobParameters
from src.helu.utils.silver_core.silver_apfel_pipeline import SilverApfelPipeline
from pyspark.sql import SparkSession

def silver_apfel_job():
    spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("SilverApfel")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    job_parameters = JobParameters(source="apfel",
                                   origin_schema="bronze",
                                   destination_schema="silver",
                                   merge_logic="s.customer_id = t.customer_id",
                                   update_condition="s.customer_id = t.customer_id",
                                   identifier="customer_id",
                                   deduplication_sorting_column="event_timestamp",
                                   partitioned_columns=["_inserted_date_utc"],
                                   columns_to_rename={
                                    "customer_uuid": "customer_id",
                                    "customer_email": "email",
                                    "customer_created_at": "signup_timestamp",
                                    "amount": "price_amount",
                                    "subscription_type": "subscription_plan",
                                    },
                                   )

    silver_pipeline = SilverApfelPipeline(spark, job_parameters)
    silver_pipeline.move_to_silver()

if __name__ == "__main__":
    silver_apfel_job()
