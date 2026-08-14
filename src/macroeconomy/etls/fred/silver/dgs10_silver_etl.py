from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class Dgs10ETL(ETLClass):

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transforma una respuesta JSON de FRED en una fila
        por observación.

        Resultado:

            date
            value
            realtime_start
            realtime_end
            year
            month
            day
            _ingested_at
            _source_file
        """

        # ------------------------------------------------------------
        # 1. Una fila por observación
        # ------------------------------------------------------------

        df = df.select(
            F.explode("observations").alias("observation"),
            "_ingested_at",
            "_source_file",
        )

        # ------------------------------------------------------------
        # 2. Extraemos los campos de la observación
        # ------------------------------------------------------------

        df = df.select(
            F.to_date(
                F.col("observation.date"),
                "yyyy-MM-dd",
            ).alias("date"),

            F.col("observation.value")
            .cast("double")
            .alias("value"),

            F.to_date(
                F.col("observation.realtime_start"),
                "yyyy-MM-dd",
            ).alias("realtime_start"),

            F.to_date(
                F.col("observation.realtime_end"),
                "yyyy-MM-dd",
            ).alias("realtime_end"),

            "_ingested_at",
            "_source_file",
        )

        # ------------------------------------------------------------
        # 3. Columnas de partición
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
        # 4. Resultado final
        # ------------------------------------------------------------

        return df.select(
            "date",
            "value",
            "realtime_start",
            "realtime_end",
            "year",
            "month",
            "day",
            "_ingested_at",
            "_source_file",
        )