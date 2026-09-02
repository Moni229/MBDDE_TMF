from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from macroeconomy.etls.etl_class import ETLClass


class FactMarketMacroETL(ETLClass):
    """Une rentabilidades de mercado con indicadores macro alineados."""

    macro_symbols = [
        "VIX",
        "US_10Y",
        "FED_FUNDS",
        "CPI_US",
        "HICP_EU",
        "INDUSTRIAL_PRODUCTION_EU",
        "BUSINESS_SENTIMENT_EU",
    ]

    def transform(
        self,
        source_dfs: dict[str, DataFrame],
        options: dict,
    ) -> DataFrame:
        if not source_dfs:
            raise ValueError("No se han proporcionado fuentes para fact_market_macro")

        market_df = None
        macro_df = None

        for _source_table, df in source_dfs.items():
            if "Adj_Close" in df.columns:
                market_df = df
            elif "frequency" in df.columns:
                macro_df = df

        if market_df is None:
            raise ValueError("No se ha encontrado fact_market_daily")

        if macro_df is None:
            raise ValueError("No se ha encontrado fact_macro")

        market_df = market_df.select(
            "symbol",
            F.to_date("Date").alias("date"),
            F.col("Adj_Close").cast("double").alias("adj_close"),
        )

        symbol_window = Window.partitionBy("symbol").orderBy("date")
        market_df = market_df.withColumn(
            "previous_adj_close", F.lag("adj_close").over(symbol_window)
        ).withColumn(
            "market_return",
            (F.col("adj_close") / F.col("previous_adj_close")) - 1,
        )

        volatility_window = (
            Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)
        )
        market_df = market_df.withColumn(
            "volatility_30d",
            F.stddev("market_return").over(volatility_window),
        )

        macro_df = macro_df.select(
            F.to_date("date").alias("date"),
            "symbol",
            "value",
        ).filter(F.col("symbol").isin(*self.macro_symbols))

        macro_df = (
            macro_df.groupBy("date")
            .pivot("symbol", self.macro_symbols)
            .agg(F.first("value"))
        )

        market_dates = market_df.select("date").distinct()
        macro_df = market_dates.join(macro_df, on="date", how="left")

        macro_window = Window.orderBy("date").rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow,
        )

        for column in self.macro_symbols:
            macro_df = macro_df.withColumn(
                column,
                F.last(F.col(column), ignorenulls=True).over(macro_window),
            )

        result = market_df.join(macro_df, on="date", how="left")

        result = (
            result.withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
        )

        return result.select(
            "date",
            "symbol",
            "adj_close",
            "market_return",
            "volatility_30d",
            *self.macro_symbols,
            "year",
            "month",
            "day",
        )
