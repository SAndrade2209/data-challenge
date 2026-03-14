from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.helu.utils.job_parameters import JobParameters
from src.helu.utils.gold_core.gold_mrr_report_pipeline import GoldMrrReportPipeline


def gold_mrr_report_job():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("GoldMRRReport")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    report_expected_schema = StructType([
        StructField("platform",           StringType(), nullable=True),
        StructField("subscription_type",  StringType(), nullable=True),
        StructField("country",            StringType(), nullable=True),
        StructField("report_month",       StringType(), nullable=True),
        StructField("original_currency",  StringType(), nullable=True),
        StructField("acquisitions",       LongType(),   nullable=False),
        StructField("renewals",           LongType(),   nullable=False),
        StructField("cancellations",      LongType(),   nullable=False),
        StructField("mrr_eur",            DoubleType(), nullable=True),
    ])
    job_parameters = JobParameters(
        source="mrr_financial_report",
        origin_schema="silver",
        destination_schema="gold",
        partitioned_columns=["report_month", "platform"],
        is_dimension=True,
        expected_schema=report_expected_schema,
    )

    pipeline = GoldMrrReportPipeline(spark, job_parameters)
    pipeline.move_to_gold()


if __name__ == "__main__":
    gold_mrr_report_job()
