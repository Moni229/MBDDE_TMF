from macroeconomy.etls.fred.silver.cipaucsl_silver_transformation import FredSilverTransformer
from macroeconomy.transformer.silver_transformer import SilverTransformer

from macroeconomy.etls.eurostat.silver.prc_hicp_midx_silver_transformation import EurostatSilverTransformer

from macroeconomy.etls.yahoo.silver.nvda_silver_transformation import YahooSilverTransformer

TRANSFORMERS = {
    "fred": FredSilverTransformer,
    "eurostat": EurostatSilverTransformer,
    "yahoo": YahooSilverTransformer,
}
def get_transformer(datasource: str) -> SilverTransformer:

    try:
        transformer_class = TRANSFORMERS[datasource]
    except KeyError:
        raise ValueError(
            f"No Silver transformer configured for "
            f"datasource '{datasource}'"
        )

    return transformer_class()