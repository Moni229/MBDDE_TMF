from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass

from macroeconomy.utils.config import load_configs


class FactMarketIntradayETL(ETLClass):

    def __init__(self, spark):
        self.spark = spark

    def transform(self, source_etl_config: dict) -> DataFrame:
        """
        Transforma los datos de mercado intradía de Silver a Gold.

        La clave del diccionario representa el símbolo del activo.
        """

        if not source_etl_config:
            raise ValueError(
                "No se han proporcionado fuentes para fact_market_intraday"
            )

        dfs = []

        for symbol, df in source_etl_config.items():
            transformed = (
                df
                .select(
                    F.lit(symbol).alias("symbol"),
                    "Date",
                    "Adj_Close",
                    "Close",
                    "High",
                    "Low",
                    "Open",
                    "Volume",
                )
            )

            dfs.append(transformed)

        if not dfs:
            raise ValueError(
                "No se han proporcionado DataFrames para fact_market_intraday"
            )

        result = dfs[0]

        for df in dfs[1:]:
            result = result.unionByName(df)

        return result