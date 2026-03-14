from loguru import logger
from pyspark.sql import DataFrame

class PipelineFunctions:
    @staticmethod
    def get_last_processing_date(df: DataFrame | None, column_name="_inserted_date_utc"):
        if (df is None) or (column_name not in df.columns) or (df.isEmpty()):
            logger.warning(f"{column_name} does not exist in dataframe or dataframe is empty. Returning None")
            return None
        return df.agg({column_name: "max"}).collect()[0][0]