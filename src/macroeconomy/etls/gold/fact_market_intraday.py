from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass

from macroeconomy.utils.constants import DEFAULT_STREAMING_WATERMARK_TIME, DEFAULT_STREAMING_WINDOW_TIME


class FactMarketIntradayETL(ETLClass):

    def transform(
        self,
        source_dfs: dict[str, DataFrame],
        options: dict
    ) -> DataFrame:
        """
        Agrega los trades de Finnhub en velas de 5 minutos.

        Entrada:
            timestamp
            symbol
            price
            volume

        Salida:
            timestamp
            symbol
            open
            high
            low
            close
            volume
            trade_count
            year
            month
            day

        El timestamp de la vela representa el inicio de la
        ventana de 5 minutos.
        """

        if not source_dfs:
            raise ValueError(
                "No se han proporcionado fuentes para "
                "fact_market_intraday"
            )

        df = next(iter(source_dfs.values()))

        # ----------------------------------------------------------
        # Watermark
        # ----------------------------------------------------------

        watermark = options.get(
            "watermark",
            DEFAULT_STREAMING_WATERMARK_TIME
        )

        df = (
            df
            .withWatermark(
                "trade_timestamp",
                watermark
            )
        )

        # ----------------------------------------------------------
        # Ventana de 5 minutos
        # ----------------------------------------------------------
        window_duration = options.get(
            "window_duration",
            DEFAULT_STREAMING_WINDOW_TIME
        )

        df = df.withColumn(
            "window",
            F.window(
                F.col("trade_timestamp"),
                window_duration
            )
        )

        # ----------------------------------------------------------
        # Agregación OHLCV
        # ----------------------------------------------------------

        result = (
            df
            .groupBy(
                "symbol",
                "window"
            )
            .agg(
                # Primer trade cronológicamente
                F.min(
                    F.struct(
                        F.col("trade_timestamp"),
                        F.col("price")
                    )
                ).alias("first_trade"),

                # Último trade cronológicamente
                F.max(
                    F.struct(
                        F.col("trade_timestamp"),
                        F.col("price")
                    )
                ).alias("last_trade"),

                # Precio máximo
                F.max("price").alias("high"),

                # Precio mínimo
                F.min("price").alias("low"),

                # Volumen total
                F.sum("volume").alias("volume"),

                # Número de trades
                F.count("*").alias("trade_count")
            )
            .select(
                F.col("window.start").alias("trade_timestamp"),
                F.col("symbol"),

                F.col("first_trade.price").alias("open"),

                F.col("high"),
                F.col("low"),

                F.col("last_trade.price").alias("close"),

                F.col("volume"),
                F.col("trade_count")
            )
        )

        # ----------------------------------------------------------
        # Particiones
        # ----------------------------------------------------------

        return (
            result
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