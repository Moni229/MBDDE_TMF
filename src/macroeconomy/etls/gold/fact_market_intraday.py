from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass
from macroeconomy.utils.constants import (
    DEFAULT_STREAMING_WATERMARK_TIME,
    DEFAULT_STREAMING_WINDOW_TIME,
)


class FactMarketIntradayETL(ETLClass):
    """Construye velas intradía de 5 minutos a partir de trades de Finnhub."""

    def transform(
        self,
        source_dfs: dict[str, DataFrame],
        options: dict,
    ) -> DataFrame:
        if not source_dfs:
            raise ValueError(
                "No se han proporcionado fuentes para " "fact_market_intraday"
            )

        df = next(iter(source_dfs.values()))

        watermark = options.get(
            "watermark",
            DEFAULT_STREAMING_WATERMARK_TIME,
        )
        df = df.withWatermark("trade_timestamp", watermark)

        window_duration = options.get(
            "window_duration",
            DEFAULT_STREAMING_WINDOW_TIME,
        )
        df = df.withColumn(
            "window",
            F.window(F.col("trade_timestamp"), window_duration),
        )

        result = (
            df.groupBy("symbol", "window")
            .agg(
                F.min(F.struct(F.col("trade_timestamp"), F.col("price"))).alias(
                    "first_trade"
                ),
                F.max(F.struct(F.col("trade_timestamp"), F.col("price"))).alias(
                    "last_trade"
                ),
                F.max("price").alias("high"),
                F.min("price").alias("low"),
                F.sum("volume").alias("volume"),
                F.count("*").alias("trade_count"),
            )
            .select(
                F.col("window.start").alias("trade_timestamp"),
                F.col("symbol"),
                F.col("first_trade.price").alias("open"),
                F.col("high"),
                F.col("low"),
                F.col("last_trade.price").alias("close"),
                F.col("volume"),
                F.col("trade_count"),
            )
        )

        return (
            result.withColumn("year", F.year("trade_timestamp"))
            .withColumn("month", F.month("trade_timestamp"))
            .withColumn("day", F.dayofmonth("trade_timestamp"))
        )
