from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class AsmlAsETL(ETLClass):

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transforma los datos diarios de Yahoo Finance
        en una estructura limpia para Silver.

        Resultado:

            date
            adj_close
            close
            high
            low
            open
            volume
            year
            month
            day
            _ingested_at
            _source_file
        """

        # ------------------------------------------------------------
        # 1. Renombramos y seleccionamos las columnas necesarias
        # ------------------------------------------------------------

        df = df.select(
            F.col("Date").alias("date"),
            F.col("Adj_Close").alias("adj_close"),
            F.col("Close").alias("close"),
            F.col("High").alias("high"),
            F.col("Low").alias("low"),
            F.col("Open").alias("open"),
            F.col("Volume").alias("volume"),
            "_ingested_at",
            "_source_file",
        )

        # ------------------------------------------------------------
        # 2. Generamos las columnas de partición
        #
        # Corresponden a la fecha del dato,
        # NO a la fecha de ingesta.
        # ------------------------------------------------------------

        df = (
            df
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
        )

        # ------------------------------------------------------------
        # 3. Resultado final
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