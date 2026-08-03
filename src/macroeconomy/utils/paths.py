import os

LANDING_CONTAINER = "landing"

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

class Settings:
    STORAGE_ACCOUNT = os.getenv("ADLS_ACCOUNT_NAME", "mastermgc001sta")
    LANDING_CONTAINER = "landing"
    BRONZE_CONTAINER = "bronze"
