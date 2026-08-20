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
        "CPIAUCSL": "fred_CPIAUCSL",
        "FEDFUNDS": "fred_FEDFUNDS",
        "DGS10": "fred_DGS10",
        "VIXCLS": "fred_VIXCLS"
    },
    "finnhub": {
        "trades": "finnhub_trades"
    }
}

DEFAULT_STREAMING_WINDOW_TIME = "5 minutes"
DEFAULT_STREAMING_WATERMARK_TIME = "5 minutes"

def get_table_name(source: str, dataset: str) -> str:
    return TABLES[source][dataset]