import os
from dataclasses import dataclass

from pyspark.sql.types import StructType

from src.helu.utils.config import (
    BRONZE,
    BRONZE_DIR,
    GOLD,
    GOLD_DIR,
    LANDING_DIR,
    SILVER,
    SILVER_DIR,
)


@dataclass
class JobParameters:
    source: str
    destination_schema: str
    origin_schema: str | None = None
    origin_path: str | None = None
    destination_path: str | None = None
    landing_schema: StructType | None = None
    expected_schema: StructType | None = None
    landing_path: str | None = None
    partitioned_columns: list | None = None
    merge_logic: str | None = None
    update_condition: str | None = None
    identifier: str | None = None
    deduplication_sorting_column: str | None = None
    is_dimension: bool | None = False
    columns_to_rename: dict | None = None
    """dict with the format: {"old_column_name": "new_column_name"} to be renamed"""

    def __post_init__(self):

        if (
            self.destination_schema != BRONZE
            and self.is_dimension is False
            and (self.identifier is None or self.deduplication_sorting_column is None)
        ):
            raise ValueError(
                "For silver and gold jobs, identifier and deduplication_sorting_column parameters must be provided for deduplication purposes."
            )

        if not self.destination_path:
            if self.destination_schema == BRONZE:
                self.destination_path = BRONZE_DIR

            if self.destination_schema == SILVER:
                self.destination_path = SILVER_DIR

            if self.destination_schema == GOLD:
                self.destination_path = GOLD_DIR

        if not self.origin_path:
            if self.origin_schema == BRONZE:
                self.origin_path = BRONZE_DIR

            if self.origin_schema == SILVER:
                self.origin_path = SILVER_DIR

            if self.origin_schema == GOLD:
                self.origin_path = GOLD_DIR

        if not self.landing_path:
            self.landing_path = os.path.join(LANDING_DIR, self.source)
