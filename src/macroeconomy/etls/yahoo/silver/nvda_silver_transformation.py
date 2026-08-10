from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.transformer.silver_transformer import SilverTransformer

class YahooSilverTransformer(SilverTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transforma los datos de Yahoo Finance a Bronze.

        Las columnas year/month/day se generan a partir de Date
        y representan la fecha del dato, no la fecha de ingesta.
        """

        # ------------------------------------------------------------
        # 1. Normalizamos nombres a lowercase
        # ------------------------------------------------------------

        df = df.toDF(*[c.lower() for c in df.columns])

        # ------------------------------------------------------------
        # 2. Generamos la fecha del dato
        # ------------------------------------------------------------

        df = df.withColumn(
            "date",
            F.to_date("date"),
        )

        # ------------------------------------------------------------
        # 3. Generamos las columnas de partición
        # ------------------------------------------------------------

        df = (
            df
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
        )

        # ------------------------------------------------------------
        # 4. Seleccionamos las columnas de Bronze
        # ------------------------------------------------------------

        return df.select(
            "date",
            "adj_close",
            "close",
            "high",
            "low",
            "open",
            "volume",
            "year",
            "month",
            "day",
            "_ingested_at",
            "_source_file",
        )