from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class FactMacroETL(ETLClass):
    """Combina indicadores macroeconómicos de Silver en Gold."""

    def transform(self, source_dfs: dict[str, DataFrame], options: dict) -> DataFrame:
        """
        Transforma los indicadores macroeconómicos de Silver a Gold.

        source_dfs:
            Diccionario con:
                nombre_tabla -> DataFrame

        options:
            Configuración específica de la ETL.
            symbols debe contener un símbolo por cada fuente,
            respetando el mismo orden que source_dfs.
        """

        if not source_dfs:
            raise ValueError("No se han proporcionado fuentes para fact_macro")

        symbols = options.get("symbols", [])

        if len(symbols) != len(source_dfs):
            raise ValueError(
                "El número de symbols debe coincidir " "con el número de fuentes"
            )

        dfs = []

        for symbol, (_source_table, df) in zip(symbols, source_dfs.items()):

            transformed = df.select(
                F.lit(symbol).alias("symbol"),
                "date",
                "value",
                "frequency",
                "geo",
                "year",
                "month",
                "day",
            )

            dfs.append(transformed)

        result = dfs[0]

        for df in dfs[1:]:
            result = result.unionByName(df)

        return result
