from src.helu.utils.landing_core.landing_ingestion import LandingIngestion

def landing_exchange_ingestion():
    exchange_rates_ingestion = LandingIngestion(
        source="exchange_rates",
        source_format="csv",
        endpoint="/exchange-rates",
        fallback_data_filename="exchange_rates")

    exchange_rates_ingestion.ingest_data_to_landing()

if __name__ == "__main__":
    landing_exchange_ingestion()
