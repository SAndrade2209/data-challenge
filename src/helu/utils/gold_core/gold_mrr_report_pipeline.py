from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from src.helu.utils.job_parameters import JobParameters
from src.helu.utils.writer import Writer


class GoldMrrReportPipeline:
    def __init__(
        self, spark: SparkSession, job_parameters: JobParameters, writer: Writer = None
    ):
        self.spark = spark
        self.job_parameters = job_parameters
        self.writer = writer if writer else Writer(spark, job_parameters)

    def read_silver_tables(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        logger.info("Reading Silver tables: apfel, fenster, exchange_rates")

        apfel_df = self.spark.read.format("delta").load(
            f"{self.job_parameters.origin_path}/apfel"
        )
        fenster_df = self.spark.read.format("delta").load(
            f"{self.job_parameters.origin_path}/fenster"
        )
        exchange_df = self.spark.read.format("delta").load(
            f"{self.job_parameters.origin_path}/exchange_rates"
        )

        return apfel_df, fenster_df, exchange_df

    def get_combined_report_query(self) -> str:
        apfer_eur_query = """
                        with apfel_eur as (
                          select
                              _source as platform,
                              subscription_plan as subscription_type,
                              country_code as country,
                              DATE_FORMAT(event_timestamp, 'yyyy-MM') as report_month,
                              CASE subscription_status
                                  WHEN 'started'   THEN 'acquisition'
                                  WHEN 'renewed'   THEN 'renewal'
                                  WHEN 'cancelled' THEN 'cancellation'
                                  ELSE subscription_status
                                  END as subscription_status,
                              price_amount,
                              renewal_period,
                              1.0 as rate_to_eur,
                              currency_norm as original_currency
                          from apfel
                          where currency_norm = 'EUR')
                        , apfel_not_eur as (
                            select
                                  a._source as platform,
                                  subscription_plan as subscription_type,
                                  country_code as country,
                                  DATE_FORMAT(event_timestamp, 'yyyy-MM') as report_month,
                                  CASE subscription_status
                                      WHEN 'started'   THEN 'acquisition'
                                      WHEN 'renewed'   THEN 'renewal'
                                      WHEN 'cancelled' THEN 'cancellation'
                                      ELSE subscription_status
                                      END as subscription_status,
                                  price_amount,
                                  renewal_period,
                                  COALESCE(e.rate_to_eur, 1.0) as rate_to_eur,
                                  a.currency_norm as original_currency
                              from apfel a
                              left join exchange_rates e
                                on  a.currency_norm = e.currency
                                and DATE_TRUNC('month', a.event_timestamp) = e.date
                              where currency_norm != 'EUR' 
                            ),
                        fenster_norm as (
                            select f._source as platform,
                               subscription_plan as subscription_type,
                               country_norm as country,
                               DATE_FORMAT(event_timestamp, 'yyyy-MM') as report_month,
                               CASE subscription_status
                                   WHEN 'new'   THEN 'acquisition'
                                   WHEN 'renew'   THEN 'renewal'
                                   WHEN 'cancel' THEN 'cancellation'
                                   ELSE subscription_status
                                   END as subscription_status,
                               price_amount,
                               renewal_period,
                               COALESCE(e.rate_to_eur, 1.0) as rate_to_eur,
                               f.currency as original_currency
                            from fenster f
                            left join exchange_rates e
                                on  f.currency = e.currency
                                and DATE_TRUNC('month', f.event_timestamp) = e.date 
                            )
                            select * from apfel_eur
                             union all
                             select * from apfel_not_eur
                             union all
                             select * from fenster_norm 
                          """
        return apfer_eur_query

    def get_combined_report(self):
        apfel_df, fenster_df, exchange_df = self.read_silver_tables()

        logger.info("Registering temp views")
        apfel_df.createOrReplaceTempView("apfel")
        fenster_df.createOrReplaceTempView("fenster")
        exchange_df.createOrReplaceTempView("exchange_rates")

        logger.info("Getting platform report dataframes")
        combined_df = self.spark.sql(self.get_combined_report_query())

        return combined_df

    def get_mrr_calculattion_query(self) -> str:
        mrr_calculated_query = """
           select
               platform,
               subscription_type,
               country,
               report_month,
               case when renewal_period = 'monthly' then price_amount
                    when renewal_period = 'yearly' then price_amount / 12
                    else price_amount end as original_monthly_amount,
               case when renewal_period = 'monthly' then round(price_amount * rate_to_eur, 2)
                    when renewal_period = 'yearly' then round(((price_amount / 12) * rate_to_eur), 2)
                    else price_amount end as eur_monthly_amount
           from combined_df
           where subscription_status in ('acquisition', 'renewal') 
       """
        return mrr_calculated_query

    def build_final_report_query(self) -> str:
        final_report_query = """
         select
             c.platform,
             c.subscription_type,
             c.country,
             c.report_month,
             c.original_currency,
             count(CASE WHEN c.subscription_status = 'acquisition'  THEN 1 END) as acquisitions,
             count(CASE WHEN c.subscription_status = 'renewal'      THEN 1 END) as renewals,
             count(CASE WHEN c.subscription_status = 'cancellation' THEN 1 END) as cancellations,
             COALESCE(SUM(m.eur_monthly_amount), 0) as mrr_eur
         from combined_df c
                  left join mrr_calculated_df m
                            on  c.platform          = m.platform
                                and c.subscription_type = m.subscription_type
                                and c.country           = m.country
                                and c.report_month      = m.report_month
         group by
             c.platform,
             c.subscription_type,
             c.country,
             c.original_currency,
             c.report_month
         order by
             c.report_month,
             c.platform,
             c.country,
             c.subscription_type 
        """

        return final_report_query

    def build_final_report(self):
        combined_df = self.get_combined_report()
        combined_df.createOrReplaceTempView("combined_df")

        logger.info("Getting MRR calculation")
        mrr_calculated_df = self.spark.sql(self.get_mrr_calculattion_query())
        mrr_calculated_df.createOrReplaceTempView("mrr_calculated_df")

        logger.info("Getting final calculation")
        final_report_df = self.spark.sql(self.build_final_report_query())
        return final_report_df

    def qa_checks(self, df_to_check: DataFrame):
        logger.info("Running QA checks on final report")
        if df_to_check and not df_to_check.isEmpty():
            total_records = df_to_check.count()
            logger.info(f"Total records in final report: {total_records}")
            if total_records == 0:
                raise ValueError("Final report is empty!")

            df_columns = df_to_check.columns
            expected_columns = {
                "platform",
                "subscription_type",
                "country",
                "report_month",
                "original_currency",
                "acquisitions",
                "renewals",
                "cancellations",
                "mrr_eur",
            }
            if not expected_columns.issubset(set(df_columns)):
                logger.error(
                    f"Final report is missing expected columns. Found columns: {df_columns}"
                )
                raise ValueError("Final report is missing expected columns!")

            if self.job_parameters.expected_schema is not None:
                expected_schema = self.job_parameters.expected_schema
                if sorted(df_to_check.dtypes) != sorted(
                    [
                        (f.name, f.dataType.simpleString())
                        for f in expected_schema.fields
                    ]
                ):
                    differences = set(df_to_check.dtypes).symmetric_difference(
                        set(
                            (f.name, f.dataType.simpleString())
                            for f in expected_schema.fields
                        )
                    )
                    logger.error(f"Schema mismatch in final report: {differences}")
                    raise ValueError(
                        f"Final report schema does not match expected schema. Differences: {differences}"
                    )

            for column in df_columns:
                null_count = df_to_check.filter(df_to_check[column].isNull()).count()
                if null_count > 0:
                    logger.warning(
                        f"Column '{column}' contains {null_count} null values."
                    )
                    raise ValueError(f"Column '{column}' contains null values!")

            logger.success(
                f"QA checks {self.job_parameters.source} passed successfully!"
            )
        else:
            logger.error("Final report dataframe is None or empty!")
            raise ValueError("Final report is empty!")

    def move_to_gold(self):
        logger.info("Starting")

        gold_df = self.build_final_report()

        self.qa_checks(df_to_check=gold_df)
        gold_df = self.writer.add_metadata_columns(df=gold_df, layer="gold")
        self.writer.create_table(df=gold_df)

        logger.info("Finished")
