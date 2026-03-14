from src.helu.utils.landing_core.landing_ingestion import LandingIngestion
from loguru import logger
import json
import csv
import io
from typing import Optional


class ApfelLandingIngestion(LandingIngestion):
    def ingest_data_to_landing(self) -> Optional[str]:
        response = self.fetch_from_api()
        if response is not None:
            logger.info(f"{self.source}: API fetch successful, saving to landing...")
            return self.save_files_to_landing(content=response.text)

        logger.info("Data fetch failed, attempting CSV fallback...")
        csv_content = self.read_csv_fallback()
        if csv_content is None:
            logger.error(f"{self.source}: CSV fallback failed, no data ingested.")
            return None

        reader = csv.DictReader(io.StringIO(csv_content))
        events = []
        for row in reader:
            if row.get("amount"):
                try:
                    row["amount"] = float(row["amount"])
                except (ValueError, TypeError):
                    pass
            if row.get("tax_amount"):
                try:
                    row["tax_amount"] = float(row["tax_amount"])
                except (ValueError, TypeError):
                    pass
            events.append(row)

        payload = {"events": events, "count": len(events)}
        content = json.dumps(payload, indent=2)
        return self.save_files_to_landing(content=content)