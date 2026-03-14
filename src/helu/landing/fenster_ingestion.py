from src.helu.utils.landing_core.landing_ingestion import LandingIngestion


def landing_fenster_ingestion():
    fenster_ingestion = LandingIngestion(
        source="fenster",
        source_format="csv",
        endpoint="/fenster/subscriptions",
        fallback_data_filename="fenster_subscriptions",
    )

    fenster_ingestion.ingest_data_to_landing()


if __name__ == "__main__":
    landing_fenster_ingestion()
