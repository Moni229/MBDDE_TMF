"""Constantes compartidas utilizadas en todo el paquete macroeconomy."""

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

CONFIG_INGESTION_FILE = "ingestion_config.yaml"
CONFIG_PIPELINE_FILE = "pipeline_config.yaml"
CONFIG_GOLD_FILE = "gold_config.yaml"

SOURCE_FORMAT_CLOUDFILES = "cloudFiles"
SOURCE_FORMAT_KAFKA = "kafka"
DELTA_FORMAT = "delta"
VALUE_FORMAT_JSON = "json"
VALUE_FORMAT_AVRO = "avro"
VALUE_FORMAT_STRING = "string"

RUN_MODE_BATCH = "batch"
RUN_MODE_STREAMING = "streaming"
RUN_MODE_AVAILABLE_NOW = "available_now"
DEFAULT_OUTPUT_MODE = "append"
WRITE_MODE_OVERWRITE = "overwrite"

DEFAULT_STREAMING_TRIGGER_TIME = "1 minute"
DEFAULT_STREAMING_WINDOW_TIME = "5 minutes"
DEFAULT_STREAMING_WATERMARK_TIME = "1 minutes"

SECRET_SCOPE = "macroeconomy"
FRED_API_KEY_SECRET = "FRED-API-KEY-2"
FINNHUB_API_KEY_SECRET = "FINNHUB-API-KEY"

DATASOURCE_CONFIG_KEY = "datasource"
FINNHUB_CONFIG_KEY = "finnhub"
YAHOO_CONFIG_KEY = "yahoo"
FRED_CONFIG_KEY = "fred"
EUROSTAT_CONFIG_KEY = "eurostat"
FINNHUB_TRADES_TOPIC = "finnhub_trades"
DEFAULT_CONFLUENT_CONFIG_PATH = "/dbfs/FileStore/client_properties"

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
        "VIXCLS": "fred_vixcls",
    },
    "finnhub": {
        "trades": "finnhub_trades",
    },
}


def get_table_name(source: str, dataset: str) -> str:
    """Devuelve el nombre canónico de tabla para un dataset de origen."""
    return TABLES[source][dataset]
