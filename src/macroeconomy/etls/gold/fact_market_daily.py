from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class FactMarketDailyETL(ETLClass):

    def transform(
        self,
        source_dfs: dict[str, DataFrame],
        options: dict
    ) -> DataFrame:
        """
        Transforma los datos diarios de mercado de Silver a Gold.

        source_dfs:
            Diccionario con:
                nombre_tabla -> DataFrame

        options:
            Configuración específica de la ETL.
        """

        if not source_dfs:
            raise ValueError(
                "No se han proporcionado fuentes para fact_market_daily"
            )

        symbols = options.get("symbols", [])

        if len(symbols) != len(source_dfs):
            raise ValueError(
                "El número de symbols debe coincidir "
                "con el número de fuentes"
            )

        dfs = []

        for symbol, (source_table, df) in zip(
            symbols,
            source_dfs.items()
        ):

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
                    "year",
                    "month",
                    "day",
                )
            )

            dfs.append(transformed)

        result = dfs[0]

        for df in dfs[1:]:
            result = result.unionByName(df)

        return result