from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class FinnhubTradesETL(ETLClass):
    """Normaliza los payloads de trades de Finnhub en filas de Silver"""

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("trade", F.explode("data"))
            .select(
                F.col("trade.s").alias("symbol"),
                F.col("trade.p").cast("double").alias("price"),
                F.col("trade.v").cast("double").alias("volume"),
                F.to_timestamp(F.from_unixtime(F.col("trade.t") / 1000)).alias(
                    "trade_timestamp"
                ),
                F.col("trade.c").alias("conditions"),
                F.col("_ingested_at"),
                F.col("_topic"),
                F.col("_partition"),
                F.col("_offset"),
            )
            .withColumn("year", F.year("trade_timestamp"))
            .withColumn("month", F.month("trade_timestamp"))
            .withColumn("day", F.dayofmonth("trade_timestamp"))
        )
