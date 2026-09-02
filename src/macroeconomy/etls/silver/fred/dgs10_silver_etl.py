from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class Dgs10ETL(ETLClass):

    def transform(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """Normaliza la serie DGS10 en filas de Silver."""

        return (
            df.withColumn(
                "observation",
                F.explode("observations"),
            )
            .select(
                F.to_date(F.col("observation.date")).alias("date"),
                F.col("observation.value").cast("double").alias("value"),
                F.lit("daily").alias("frequency"),
                F.lit("US").alias("geo"),
                F.to_date(F.col("observation.realtime_start")).alias("realtime_start"),
                F.to_date(F.col("observation.realtime_end")).alias("realtime_end"),
                "_ingested_at",
                "_source_file",
            )
            .withColumn(
                "year",
                F.year("date"),
            )
            .withColumn(
                "month",
                F.month("date"),
            )
            .withColumn(
                "day",
                F.dayofmonth("date"),
            )
        )
