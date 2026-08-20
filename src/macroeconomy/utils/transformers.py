from macroeconomy.etls.silver.fred.cipaucsl_silver_etl import CipaucslETL
from macroeconomy.etls.silver.fred.fedfunds_silver_etl import FedFundsETL
from macroeconomy.etls.silver.fred.dgs10_silver_etl import Dgs10ETL
from macroeconomy.etls.silver.fred.vixcls_silver_etl import VixclsETL
from macroeconomy.etls.silver.eurostat.prc_hicp_manr_silver_etl import PrcHipcManrETL
from macroeconomy.etls.silver.eurostat.sts_inpr_m_silver_etl import StsInprMETL
from macroeconomy.etls.silver.eurostat.ei_bssi_m_r2_silver_etl import EiBssiMR2ETL
from macroeconomy.etls.silver.yahoo.nvda_silver_etl import NvdaETL

from macroeconomy.etls.etl_class import ETLClass

from macroeconomy.etls.silver.yahoo.asml_silver_etl import AsmlAsETL
from macroeconomy.etls.silver.yahoo.ndx_silver_etl import NdxETL

from macroeconomy.etls.gold.fact_market_intraday import FactMarketIntradayETL

from macroeconomy.etls.silver.finnhub.finnhub_trades_etl import FinnhubTradesETL

from macroeconomy.etls.gold.fact_market_daily import FactMarketDailyETL

SILVER_ETL_REGISTRY = {
    "fred_CPIAUCSL": CipaucslETL,
    "fred_FEDFUNDS": FedFundsETL,
    "fred_DGS10": Dgs10ETL,
    "fred_VIXCLS": VixclsETL,
    "eurostat_prc_hicp_manr": PrcHipcManrETL,
    "eurostat_sts_inpr_m": StsInprMETL,
    "eurostat_ei_bssi_m_r2": EiBssiMR2ETL,
    "yahoo_nvda": NvdaETL,
    "yahoo_asml": AsmlAsETL,
    "yahoo_ndx": NdxETL,
    "finnhub_trades": FinnhubTradesETL,
}

GOLD_ETL_REGISTRY = {
    "fact_market_intraday": FactMarketIntradayETL,
    "fact_market_daily": FactMarketDailyETL
}

def get_etl(layer: str, datasource: str) -> ETLClass:

    registries = {
        "silver": SILVER_ETL_REGISTRY,
        "gold": GOLD_ETL_REGISTRY,
    }

    try:
        registry = registries[layer.lower()]
    except KeyError:
        raise ValueError(
            f"Unsupported layer '{layer}'. "
            f"Expected one of: {list(registries.keys())}"
        )

    try:
        transformer_class = registry[datasource]
    except KeyError:
        raise ValueError(
            f"No {layer.upper()} transformer configured for "
            f"datasource '{datasource}'"
        )

    return transformer_class()