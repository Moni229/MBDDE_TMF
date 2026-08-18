from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class FinnhubTradesETL(ETLClass):

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df
            # Un registro por trade
            .withColumn(
                "trade",
                F.explode("data")
            )

            # Extraer campos Finnhub
            .select(
                F.col("trade.s").alias("symbol"),

                F.col("trade.p")
                .cast("double")
                .alias("price"),

                F.col("trade.v")
                .cast("double")
                .alias("volume"),

                F.to_timestamp(
                    F.from_unixtime(
                        F.col("trade.t") / 1000
                    )
                ).alias("trade_timestamp"),

                F.col("trade.c").alias("conditions"),

                F.col("_ingested_at"),

                # Metadata Kafka
                F.col("_topic"),
                F.col("_partition"),
                F.col("_offset"),
            )

            # Particiones temporales
            .withColumn(
                "year",
                F.year("trade_timestamp")
            )
            .withColumn(
                "month",
                F.month("trade_timestamp")
            )
            .withColumn(
                "day",
                F.dayofmonth("trade_timestamp")
            )
        )