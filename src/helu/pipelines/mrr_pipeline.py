from loguru import logger

# Landing
from src.helu.landing.apfel_ingestion import landing_apfel_ingestion
from src.helu.landing.fenster_ingestion import landing_fenster_ingestion
from src.helu.landing.exchange_rate_ingestion import landing_exchange_ingestion

# Bronze
from src.helu.bronze.bronze_apfel import bronze_apfel_job
from src.helu.bronze.bronze_fenster import bronze_fenster_job
from src.helu.bronze.bronze_exchange_rate import bronze_exchange_job

# Silver
from src.helu.silver.silver_apfel import silver_apfel_job
from src.helu.silver.silver_fenster import silver_fenster_job
from src.helu.silver.silver_exchange_rate import silver_exchange_job

# Gold
from src.helu.gold.gold_mrr_report import gold_mrr_report_job


def apfel_pipeline():
    landing_apfel_ingestion()
    bronze_apfel_job()
    silver_apfel_job()


def fenster_pipeline():
    landing_fenster_ingestion()
    bronze_fenster_job()
    silver_fenster_job()


def exchange_rates_pipeline():
    landing_exchange_ingestion()
    bronze_exchange_job()
    silver_exchange_job()


def run_full_pipeline():
    logger.info("Starting MRR pipeline")

    logger.info("Starting apfel pipeline")
    apfel_pipeline()
    logger.info("Starting fenster pipeline")
    fenster_pipeline()
    logger.info("Starting exchange pipeline")
    exchange_rates_pipeline()

    logger.info("Getting MRR report")
    gold_mrr_report_job()

    logger.success("Done")


if __name__ == "__main__":
    run_full_pipeline()
