import os
from pyspark.sql import SparkSession

LANDING_CONTAINER = "landing"
LAKEHOUSE_CONTAINER = "lakehouse"

def get_landing_root() -> str:
    return f"abfss://{LANDING_CONTAINER}@{Settings.STORAGE_ACCOUNT}.dfs.core.windows.net/macroeconomy"

def get_landing_paths() -> dict[str, str]:
    """Returns the landing zone path for each source dataset."""
    root = get_landing_root()
    return {
        "fred": f"{root}/fred",
        "yahoo": f"{root}/yahoo",
        "eurostat": f"{root}/eurostat",
        "finnhub": f"{root}/finnhub"
    }

def get_bronze_source_paths() -> dict[str, str]:
    """Returns the bronze zone path for each source dataset."""
    root = get_landing_root()
    return {
        "fred": f"{root}/fred",
        "yahoo": f"{root}/yahoo",
        "eurostat": f"{root}/eurostat",
        "finnhub": f"{root}/finnhub"
    }

def _get_spark() -> SparkSession:
    return SparkSession.getActiveSession()

def get_catalog() -> str:
    return _get_spark().catalog.currentCatalog()

def get_schemas() -> dict[str, str]:
    catalog = get_catalog()
    return {
        "bronze": f"{catalog}.macroeconomy_bronze"
    }


def get_bronze_root() -> str:
    return f"abfss://{LAKEHOUSE_CONTAINER}@{Settings.STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/macroeconomy"


class Settings:
    STORAGE_ACCOUNT = os.getenv("ADLS_ACCOUNT_NAME", "mastermgc001sta")
    LANDING_CONTAINER = "landing"
    BRONZE_CONTAINER = "bronze"
