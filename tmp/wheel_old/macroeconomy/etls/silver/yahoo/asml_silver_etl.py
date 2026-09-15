from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class AsmlAsETL(ETLClass):
    """Normaliza los datos de Yahoo Finance de ASML en filas de Silver"""

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.select(
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
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
            .select(
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
        )
