from src.helu.utils.landing_core.apfel_ingestion_pipeline import ApfelLandingIngestion


def landing_apfel_ingestion():
    apfel_ingestion = ApfelLandingIngestion(
        source="apfel",
        source_format="json",
        endpoint="/apfel/subscriptions",
        fallback_data_filename="apfel_subscriptions",
    )

    apfel_ingestion.ingest_data_to_landing()


if __name__ == "__main__":
    landing_apfel_ingestion()
