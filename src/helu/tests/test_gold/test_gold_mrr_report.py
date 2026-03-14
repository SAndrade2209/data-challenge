from unittest import TestCase
from mock import patch
from datetime import datetime, date
from src.helu.utils.job_parameters import JobParameters
from src.helu.utils.gold_core.gold_mrr_report_pipeline import GoldMrrReportPipeline
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StructField,
    StructType,
    TimestampType,
    StringType,
)
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual


class TestMaterializedViewWorkExperiences(TestCase):
    @patch("pyspark.sql.SparkSession")
    @patch("src.helu.utils.writer")
    def setUp(self, mock_writer, mock_spark_session):
        self.mock_spark_session = mock_spark_session
        self.writer = mock_writer
        self.sql_context = (
            SparkSession.builder.master("local[1]")  # type: ignore
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.rdd.compress", "false")
            .config("spark.shuffle.compress", "false")
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.executor.cores", 1)
            .config("spark.executor.instances", 1)
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.driver.host", "localhost")
            .appName("UnittestSpark")
            .getOrCreate()
        )
        self.spark = self.mock_spark_session.return_value

        self.report_expected_schema = StructType(
            [
                StructField("platform", StringType(), nullable=True),
                StructField("subscription_type", StringType(), nullable=True),
                StructField("country", StringType(), nullable=True),
                StructField("report_month", StringType(), nullable=True),
                StructField("original_currency", StringType(), nullable=True),
                StructField("acquisitions", LongType(), nullable=False),
                StructField("renewals", LongType(), nullable=False),
                StructField("cancellations", LongType(), nullable=False),
                StructField("mrr_eur", DoubleType(), nullable=True),
            ]
        )

        job_parameters = JobParameters(
            source="report_test",
            origin_schema="silver",
            destination_schema="gold",
            partitioned_columns=["report_month", "platform"],
            is_dimension=True,
        )

        self.pipeline = GoldMrrReportPipeline(
            spark=self.spark, job_parameters=job_parameters, writer=self.writer
        )

        silver_apfel_schema = StructType(
            [
                StructField("_source", StringType(), True),
                StructField("subscription_plan", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("event_timestamp", TimestampType(), True),
                StructField("subscription_status", StringType(), True),
                StructField("price_amount", DoubleType(), True),
                StructField("renewal_period", StringType(), True),
                StructField("currency_norm", StringType(), True),
            ]
        )
        silver_apfel_data = [
            Row(
                _source="apfel",
                subscription_plan="standard",
                country_code="DE",
                event_timestamp=datetime(2025, 2, 15, 10, 0, 0),
                subscription_status="started",
                price_amount=10.0,
                renewal_period="monthly",
                currency_norm="EUR",
            ),
            Row(
                _source="apfel",
                subscription_plan="standard",
                country_code="DE",
                event_timestamp=datetime(2025, 3, 15, 10, 0, 0),
                subscription_status="started",
                price_amount=15.0,
                renewal_period="yearly",
                currency_norm="EUR",
            ),
            Row(
                _source="apfel",
                subscription_plan="premium",
                country_code="GB",
                event_timestamp=datetime(2025, 4, 15, 10, 0, 0),
                subscription_status="cancelled",
                price_amount=10.0,
                renewal_period="monthly",
                currency_norm="USD",
            ),
            Row(
                _source="apfel",
                subscription_plan="standard",
                country_code="DE",
                event_timestamp=datetime(2025, 5, 15, 10, 0, 0),
                subscription_status="renewed",
                price_amount=12.0,
                renewal_period="yearly",
                currency_norm="USD",
            ),
        ]

        self.apfel_df = self.sql_context.createDataFrame(
            silver_apfel_data, schema=silver_apfel_schema
        )

        silver_fenster_schema = StructType(
            [
                StructField("_source", StringType(), True),
                StructField("subscription_plan", StringType(), True),
                StructField("country_norm", StringType(), True),
                StructField("event_timestamp", TimestampType(), True),
                StructField("subscription_status", StringType(), True),
                StructField("price_amount", DoubleType(), True),
                StructField("renewal_period", StringType(), True),
                StructField("currency", StringType(), True),
            ]
        )

        silver_fenster_data = [
            Row(
                _source="fenster",
                subscription_plan="standard",
                country_norm="USA",
                event_timestamp=datetime(2025, 2, 15, 10, 0, 0),
                subscription_status="new",
                price_amount=10.0,
                renewal_period="monthly",
                currency="USD",
            ),
            Row(
                _source="fenster",
                subscription_plan="premium",
                country_norm="USA",
                event_timestamp=datetime(2025, 3, 15, 10, 0, 0),
                subscription_status="renew",
                price_amount=12.0,
                renewal_period="yearly",
                currency="USD",
            ),
            Row(
                _source="fenster",
                subscription_plan="premium",
                country_norm="GB",
                event_timestamp=datetime(2025, 4, 15, 10, 0, 0),
                subscription_status="cancel",
                price_amount=10.0,
                renewal_period="monthly",
                currency="USD",
            ),
        ]

        self.fenster_df = self.sql_context.createDataFrame(
            silver_fenster_data, schema=silver_fenster_schema
        )

        silver_exchange_schema = StructType(
            [
                StructField("date", DateType(), True),
                StructField("currency", StringType(), True),
                StructField("rate_to_eur", DoubleType(), True),
            ]
        )

        silver_exchange_data = [
            Row(date=date(2025, 2, 1), currency="USD", rate_to_eur=0.2),
            Row(date=date(2025, 3, 1), currency="USD", rate_to_eur=0.3),
            Row(date=date(2025, 4, 1), currency="USD", rate_to_eur=0.4),
            Row(date=date(2025, 5, 1), currency="USD", rate_to_eur=0.5),
            Row(date=date(2025, 6, 1), currency="USD", rate_to_eur=0.6),
        ]

        self.exchange_rate_df = self.sql_context.createDataFrame(
            silver_exchange_data, schema=silver_exchange_schema
        )

        combined_expected_schema = StructType(
            [
                StructField("platform", StringType(), True),
                StructField("subscription_type", StringType(), True),
                StructField("country", StringType(), True),
                StructField("report_month", StringType(), True),
                StructField("subscription_status", StringType(), True),
                StructField("price_amount", DoubleType(), True),
                StructField("renewal_period", StringType(), True),
                StructField("rate_to_eur", DoubleType(), True),
                StructField("original_currency", StringType(), True),
            ]
        )


        combined_expected_data = [
            Row(
                platform="apfel",
                subscription_type="standard",
                country="DE",
                report_month="2025-02",
                subscription_status="acquisition",
                price_amount=10.0,
                renewal_period="monthly",
                rate_to_eur=1.0,
                original_currency="EUR",
            ),
            Row(
                platform="apfel",
                subscription_type="standard",
                country="DE",
                report_month="2025-03",
                subscription_status="acquisition",
                price_amount=15.0,
                renewal_period="yearly",
                rate_to_eur=1.0,
                original_currency="EUR",
            ),
            Row(
                platform="apfel",
                subscription_type="premium",
                country="GB",
                report_month="2025-04",
                subscription_status="cancellation",
                price_amount=10.0,
                renewal_period="monthly",
                rate_to_eur=0.4,
                original_currency="USD",
            ),
            Row(
                platform="apfel",
                subscription_type="standard",
                country="DE",
                report_month="2025-05",
                subscription_status="renewal",
                price_amount=12.0,
                renewal_period="yearly",
                rate_to_eur=0.5,
                original_currency="USD",
            ),
            Row(
                platform="fenster",
                subscription_type="standard",
                country="USA",
                report_month="2025-02",
                subscription_status="acquisition",
                price_amount=10.0,
                renewal_period="monthly",
                rate_to_eur=0.2,
                original_currency="USD",
            ),
            Row(
                platform="fenster",
                subscription_type="premium",
                country="USA",
                report_month="2025-03",
                subscription_status="renewal",
                price_amount=12.0,
                renewal_period="yearly",
                rate_to_eur=0.3,
                original_currency="USD",
            ),
            Row(
                platform="fenster",
                subscription_type="premium",
                country="GB",
                report_month="2025-04",
                subscription_status="cancellation",
                price_amount=10.0,
                renewal_period="monthly",
                rate_to_eur=0.4,
                original_currency="USD",
            ),
        ]

        self.combined_expected_df = self.sql_context.createDataFrame(
            combined_expected_data, schema=combined_expected_schema
        )

    @patch.object(GoldMrrReportPipeline, "read_silver_tables")
    def test_get_combined_report(self, mock_read_silver_tables):
        self.apfel_df.createOrReplaceTempView("apfel")
        self.fenster_df.createOrReplaceTempView("fenster")
        self.exchange_rate_df.createOrReplaceTempView("exchange_rates")

        self.pipeline.spark = self.sql_context
        mock_read_silver_tables.return_value = (self.apfel_df, self.fenster_df, self.exchange_rate_df)

        combined_df = self.pipeline.get_combined_report()

        assertDataFrameEqual(combined_df, self.combined_expected_df)

    @patch.object(GoldMrrReportPipeline, "get_combined_report")
    def test_build_final_report(self, mock_get_combined_report):
        mock_get_combined_report.return_value = self.combined_expected_df
        self.pipeline.spark = self.sql_context

        final_report_expected_schema = StructType(
            [
                StructField("platform", StringType(), True),
                StructField("subscription_type", StringType(), True),
                StructField("country", StringType(), True),
                StructField("report_month", StringType(), True),
                StructField("original_currency", StringType(), True),
                StructField("acquisitions", LongType(), False),
                StructField("renewals", LongType(), False),
                StructField("cancellations", LongType(), False),
                StructField("mrr_eur", DoubleType(), True),
            ]
        )

        final_report_expected_data = [
            # apfel DE EUR standard — 2 acquisitions (Feb + Mar), 0 renewals, 0 cancellations
            Row(platform="apfel", subscription_type="standard", country="DE", report_month="2025-02", original_currency="EUR", acquisitions=1, renewals=0, cancellations=0, mrr_eur=10.0),
            Row(platform="apfel", subscription_type="standard", country="DE", report_month="2025-03", original_currency="EUR", acquisitions=1, renewals=0, cancellations=0, mrr_eur=15.0 / 12),
            # apfel DE USD standard — 0 acquisitions, 1 renewal, 0 cancellations
            Row(platform="apfel", subscription_type="standard", country="DE", report_month="2025-05", original_currency="USD", acquisitions=0, renewals=1, cancellations=0, mrr_eur=round((12.0 / 12) * 0.5, 2)),
            # apfel GB USD premium — 0 acquisitions, 0 renewals, 1 cancellation
            Row(platform="apfel", subscription_type="premium", country="GB", report_month="2025-04", original_currency="USD", acquisitions=0, renewals=0, cancellations=1, mrr_eur=0.0),
            # fenster USA USD standard — 1 acquisition
            Row(platform="fenster", subscription_type="standard", country="USA", report_month="2025-02", original_currency="USD", acquisitions=1, renewals=0, cancellations=0, mrr_eur=round(10.0 * 0.2, 2)),
            # fenster USA USD premium — 1 renewal
            Row(platform="fenster", subscription_type="premium", country="USA", report_month="2025-03", original_currency="USD", acquisitions=0, renewals=1, cancellations=0, mrr_eur=round((12.0 / 12) * 0.3, 2)),
            # fenster GB USD premium — 1 cancellation
            Row(platform="fenster", subscription_type="premium", country="GB", report_month="2025-04", original_currency="USD", acquisitions=0, renewals=0, cancellations=1, mrr_eur=0.0),
        ]

        final_report_expected_df = self.sql_context.createDataFrame(
            final_report_expected_data, schema=final_report_expected_schema
        )

        result_df = self.pipeline.build_final_report()

        assertDataFrameEqual(result_df, final_report_expected_df)

    def test_qa_checks_passes_with_valid_dataframe(self):
        valid_schema = StructType(
            [
                StructField("platform", StringType(), True),
                StructField("subscription_type", StringType(), True),
                StructField("country", StringType(), True),
                StructField("report_month", StringType(), True),
                StructField("original_currency", StringType(), True),
                StructField("acquisitions", LongType(), False),
                StructField("renewals", LongType(), False),
                StructField("cancellations", LongType(), False),
                StructField("mrr_eur", DoubleType(), True),
            ]
        )
        valid_data = [
            Row(platform="apfel", subscription_type="standard", country="DE", report_month="2025-02", original_currency="EUR", acquisitions=1, renewals=0, cancellations=0, mrr_eur=10.0),
        ]
        valid_df = self.sql_context.createDataFrame(valid_data, schema=valid_schema)

        # Act & Assert — should not raise
        try:
            self.pipeline.qa_checks(df_to_check=valid_df)
        except Exception as e:
            self.fail(f"qa_checks raised an unexpected exception: {e}")

    def test_qa_checks_raises_on_empty_dataframe(self):
        valid_schema = StructType(
            [
                StructField("platform", StringType(), True),
                StructField("subscription_type", StringType(), True),
                StructField("country", StringType(), True),
                StructField("report_month", StringType(), True),
                StructField("original_currency", StringType(), True),
                StructField("acquisitions", LongType(), False),
                StructField("renewals", LongType(), False),
                StructField("cancellations", LongType(), False),
                StructField("mrr_eur", DoubleType(), True),
            ]
        )
        empty_df = self.sql_context.createDataFrame([], schema=valid_schema)

        with self.assertRaises(Exception):
            self.pipeline.qa_checks(df_to_check=empty_df)

    def test_qa_checks_raises_on_missing_columns(self):
        incomplete_schema = StructType(
            [
                StructField("platform", StringType(), True),
                StructField("subscription_type", StringType(), True),
                StructField("country", StringType(), True),
                StructField("report_month", StringType(), True),
            ]
        )
        incomplete_data = [
            Row(platform="apfel", subscription_type="standard", country="DE", report_month="2025-02"),
        ]
        incomplete_df = self.sql_context.createDataFrame(incomplete_data, schema=incomplete_schema)

        with self.assertRaises(Exception):
            self.pipeline.qa_checks(df_to_check=incomplete_df)

    def test_qa_checks_passes_with_matching_expected_schema(self):
        job_parameters_with_schema = JobParameters(
            source="report_test",
            origin_schema="silver",
            destination_schema="gold",
            partitioned_columns=["report_month", "platform"],
            is_dimension=True,
            expected_schema=self.report_expected_schema,
        )
        pipeline_with_schema = GoldMrrReportPipeline(
            spark=self.spark, job_parameters=job_parameters_with_schema, writer=self.writer
        )

        valid_df = self.sql_context.createDataFrame(
            [Row(platform="apfel", subscription_type="standard", country="DE", report_month="2025-02", original_currency="EUR", acquisitions=1, renewals=0, cancellations=0, mrr_eur=10.0)],
            schema=self.report_expected_schema,
        )

        # Act & Assert — should not raise because the schema matches
        try:
            pipeline_with_schema.qa_checks(df_to_check=valid_df)
        except Exception as e:
            self.fail(f"qa_checks raised an unexpected exception with a matching schema: {e}")

    def test_qa_checks_raises_on_schema_mismatch(self):
        job_parameters_with_schema = JobParameters(
            source="report_test",
            origin_schema="silver",
            destination_schema="gold",
            partitioned_columns=["report_month", "platform"],
            is_dimension=True,
            expected_schema=self.report_expected_schema,
        )
        pipeline_with_schema = GoldMrrReportPipeline(
            spark=self.spark, job_parameters=job_parameters_with_schema, writer=self.writer
        )

        wrong_schema = StructType(
            [
                StructField("platform", StringType(), True),
                StructField("subscription_type", StringType(), True),
                StructField("country", StringType(), True),
                StructField("report_month", StringType(), True),
                StructField("original_currency", StringType(), True),
                StructField("acquisitions", LongType(), False),
                StructField("renewals", LongType(), False),
                StructField("cancellations", LongType(), False),
                StructField("mrr_eur", StringType(), True),  # wrong type
            ]
        )
        wrong_schema_df = self.sql_context.createDataFrame(
            [Row(platform="apfel", subscription_type="standard", country="DE", report_month="2025-02", original_currency="EUR", acquisitions=1, renewals=0, cancellations=0, mrr_eur="10.0")],
            schema=wrong_schema,
        )

        with self.assertRaises(ValueError):
            pipeline_with_schema.qa_checks(df_to_check=wrong_schema_df)

