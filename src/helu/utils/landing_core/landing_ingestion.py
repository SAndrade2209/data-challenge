import os
from datetime import datetime, timezone
from typing import Optional, Union

import requests
from loguru import logger

from src.helu.utils.config import API_BASE_URL, DATA_DIR, LANDING_DIR
from src.helu.utils.definitions_enums import SourceFormat


class LandingIngestion:
    def __init__(
        self,
        source: str,
        source_format: Union[str, SourceFormat],
        endpoint: str = None,
        fallback_data_filename: str = None,
    ):
        self.source = source
        self.source_format = (
            source_format
            if isinstance(source_format, SourceFormat)
            else SourceFormat.from_string(source_format)
        )
        self.source_inbound_dir_name = source.replace("-", "_")
        self.data_filename = fallback_data_filename
        self.endpoint = f"{endpoint.strip()}" if endpoint else f"/{self.source}"
        self.data_filename = fallback_data_filename

    def fetch_from_api(self, timeout: int = 30) -> Optional[requests.Response]:
        url = f"{API_BASE_URL}{self.endpoint}"
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            logger.info(
                f"API OK: {url} -> {response.status_code} ({len(response.content)} bytes)"
            )
            return response
        except requests.exceptions.RequestException as e:
            logger.warning(f"API FAIL: {url} -> {e}")
            return None

    def read_csv_fallback(self) -> Optional[str]:
        csv_filename = (
            f"{self.data_filename}.csv"
            if self.data_filename
            else f"{self.source_inbound_dir_name}.csv"
        )
        filepath = os.path.join(DATA_DIR, csv_filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
            logger.info(f"CSV FALLBACK OK: {filepath} ({len(content)} bytes)")
            return content
        except FileNotFoundError:
            logger.error(f"CSV FALLBACK FAIL: {filepath} not found")
            return None

    def save_files_to_landing(self, content: str) -> str:
        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y-%m-%d")
        ts = now.strftime("%Y-%m-%dT%H-%M-%S")
        filename = f"{self.source}_{ts}.{self.source_format.value}"

        source_dir = os.path.join(LANDING_DIR, self.source, date_partition)
        os.makedirs(source_dir, exist_ok=True)

        filepath = os.path.join(source_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"LANDING SAVED: {filepath}")
        return filepath

    def ingest_data_to_landing(self) -> Optional[str]:
        logger.info("Reading from API...")
        response = self.fetch_from_api()
        if response is not None:
            logger.info(f"{self.source}: API fetch successful, saving to landing...")
            return self.save_files_to_landing(content=response.text)

        logger.info("Data fetch failed, attempting CSV fallback...")
        csv_content = self.read_csv_fallback()
        if csv_content is None:
            logger.error(f"{self.source}: CSV fallback failed, no data ingested.")
            return None

        return self.save_files_to_landing(content=csv_content)
