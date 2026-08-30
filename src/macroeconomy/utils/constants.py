BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
TABLES = {
    "yahoo": {
        "NVDA": "yahoo_nvda",
        "ASML.AS": "yahoo_asml",
        "^NDX": "yahoo_ndx",
    },
    "eurostat": {
        "prc_hicp_manr": "eurostat_prc_hicp_manr",
        "sts_inpr_m": "eurostat_sts_inpr_m",
        "ei_bssi_m_r2": "eurostat_ei_bssi_m_r2",
    },
    "fred": {
        "CPIAUCSL": "fred_cpiaucsl",
        "FEDFUNDS": "fred_fedfunds",
        "DGS10": "fred_dgs10",
        "VIXCLS": "fred_vixcls"
    },
    "finnhub": {
        "trades": "finnhub_trades"
    }
}

DEFAULT_STREAMING_WINDOW_TIME = "5 minutes"
DEFAULT_STREAMING_WATERMARK_TIME = "5 minutes"
DEFAULT_MODE = "append"
DEFAULT_RUN_MODE = "batch"

SECRET_SCOPE = "macroeconomy"
FRED_API_KEY_SECRET = "FRED-API-KEY-2"
FINNHUB_API_KEY_SECRET = "FINNHUB-API-KEY"

def get_table_name(source: str, dataset: str) -> str:
    return TABLES[source][dataset]