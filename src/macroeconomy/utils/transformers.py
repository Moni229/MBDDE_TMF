from macroeconomy.etls.fred.silver.cipaucsl_silver_etl import CipaucslETL
from macroeconomy.etls.fred.silver.fedfunds_silver_etl import FedFundsETL
from macroeconomy.etls.fred.silver.dgs10_silver_etl import Dgs10ETL
from macroeconomy.etls.fred.silver.vixcls_silver_etl import VixclsETL
from macroeconomy.etls.eurostat.silver.prc_hicp_manr_silver_etl import PrcHipcManrETL
from macroeconomy.etls.eurostat.silver.sts_inpr_m_silver_etl import StsInprMETL
from macroeconomy.etls.eurostat.silver.ei_bssi_m_r2_silver_etl import EiBssiMR2ETL
from macroeconomy.etls.yahoo.silver.nvda_silver_etl import NvdaETL

from macroeconomy.etls.etl_class import ETLClass

from macroeconomy.etls.yahoo.silver.asml_silver_etl import AsmlAsETL
from macroeconomy.etls.yahoo.silver.ndx_silver_etl import NdxETL

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
    "yahoo_ndx": NdxETL
}
def get_etl(datasource: str) -> ETLClass:

    try:
        transformer_class = SILVER_ETL_REGISTRY[datasource]
    except KeyError:
        raise ValueError(
            f"No Silver transformer configured for "
            f"datasource '{datasource}'"
        )

    return transformer_class()