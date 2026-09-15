from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from macroeconomy.etls.etl_class import ETLClass


class FactMacroETL(ETLClass):
    """Combina indicadores macroeconómicos de Silver en Gold."""

    def transform(self, source_dfs: dict[str, DataFrame], options: dict) -> DataFrame:
        """
        Transforma los indicadores macroeconómicos de Silver a Gold.

        Para CPI USA calcula la variación interanual a partir
        del índice mensual.

        HICP UE ya contiene directamente la variación interanual.
        """

        if not source_dfs:
            raise ValueError("No se han proporcionado fuentes para fact_macro")

        symbols = options.get("symbols", [])

        if len(symbols) != len(source_dfs):
            raise ValueError(
                "El número de symbols debe coincidir con el número de fuentes"
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

            # CPI USA: calcular variación interanual
            if symbol == "CPI_US":

                window = Window.partitionBy("geo").orderBy("date")

                transformed = (
                    transformed
                    .withColumn(
                        "previous_year_value",
                        F.lag("value", 12).over(window),
                    )
                    .withColumn(
                        "value",
                        F.when(
                            F.col("previous_year_value").isNotNull()
                            & (F.col("previous_year_value") != 0),
                            (
                                    F.col("value") / F.col("previous_year_value") - 1
                            ) * 100,
                        ).otherwise(None),
                    )
                    .drop("previous_year_value")
                )
            dfs.append(transformed)

        result = dfs[0]

        for df in dfs[1:]:
            result = result.unionByName(df)

        return result