from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from macroeconomy.etls.etl_class import ETLClass


class FactMarketMacroETL(ETLClass):
    """Une datos mensuales de mercado con indicadores macroeconómicos."""

    macro_symbols = [
        "VIX",
        "US_10Y",
        "FED_FUNDS",
        "CPI_US",
        "HICP_EU",
        "INDUSTRIAL_PRODUCTION_EU",
        "BUSINESS_SENTIMENT_EU",
    ]

    daily_macro_symbols = [
        "VIX",
        "US_10Y",
    ]

    monthly_macro_symbols = [
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
            raise ValueError(
                "No se han proporcionado fuentes para fact_market_macro"
            )

        market_df = None
        macro_df = None

        for _, df in source_dfs.items():
            if "Adj_Close" in df.columns:
                market_df = df

            elif "frequency" in df.columns:
                macro_df = df

        if market_df is None:
            raise ValueError(
                "No se ha encontrado fact_market_daily"
            )

        if macro_df is None:
            raise ValueError(
                "No se ha encontrado fact_macro"
            )

        # ============================================================
        # 1. MERCADO: DIARIO -> MENSUAL
        # ============================================================

        market_df = (
            market_df
            .select(
                "symbol",
                F.to_date("Date").alias("date"),
                F.col("Adj_Close").cast("double").alias("adj_close"),
            )
            .withColumn(
                "month_date",
                F.to_date(
                    F.date_trunc("month", F.col("date"))
                )
            )
        )

        # Último día disponible de cada mes para cada activo.
        last_market_day_window = (
            Window
            .partitionBy("symbol", "month_date")
            .orderBy(F.col("date").desc())
        )

        market_monthly = (
            market_df
            .withColumn(
                "_row_number",
                F.row_number().over(last_market_day_window),
            )
            .filter(F.col("_row_number") == 1)
            .drop("_row_number", "date")
            .withColumnRenamed("month_date", "date")
        )

        # Rentabilidad mensual: cierre del mes actual
        # frente al cierre del mes anterior.
        market_return_window = (
            Window
            .partitionBy("symbol")
            .orderBy("date")
        )

        market_monthly = (
            market_monthly
            .withColumn(
                "previous_adj_close",
                F.lag("adj_close").over(
                    market_return_window
                ),
            )
            .withColumn(
                "market_return",
                (
                    (
                        F.col("adj_close")
                        / F.col("previous_adj_close")
                    )
                    - 1
                ) * 100,
            )
            .drop("previous_adj_close")
        )

        # ============================================================
        # 2. MACRO: PREPARACIÓN
        # ============================================================

        macro_df = (
            macro_df
            .select(
                F.to_date("date").alias("date"),
                "symbol",
                F.col("value").cast("double").alias("value"),
            )
            .filter(
                F.col("symbol").isin(
                    *self.macro_symbols
                )
            )
            .withColumn(
                "month_date",
                F.to_date(
                    F.date_trunc(
                        "month",
                        F.col("date"),
                    )
                )
            )
        )

        # ============================================================
        # 3. VARIABLES DIARIAS -> MEDIA MENSUAL
        #
        # VIX
        # US_10Y
        # ============================================================

        daily_macro_monthly = (
            macro_df
            .filter(
                F.col("symbol").isin(
                    *self.daily_macro_symbols
                )
            )
            .groupBy(
                "month_date",
                "symbol",
            )
            .agg(
                F.avg("value").alias("value")
            )
        )

        # ============================================================
        # 4. VARIABLES YA MENSUALES
        #
        # Se conserva el valor mensual.
        # ============================================================

        monthly_macro = (
            macro_df
            .filter(
                F.col("symbol").isin(
                    *self.monthly_macro_symbols
                )
            )
            .select(
                "month_date",
                "symbol",
                "value",
            )
        )

        # ============================================================
        # 5. INDUSTRIAL PRODUCTION:
        # ÍNDICE -> VARIACIÓN INTERANUAL (%)
        # ============================================================

        industrial_production = (
            monthly_macro
            .filter(
                F.col("symbol")
                == "INDUSTRIAL_PRODUCTION_EU"
            )
        )

        industrial_window = (
            Window
            .partitionBy("symbol")
            .orderBy("month_date")
        )

        industrial_production = (
            industrial_production
            .withColumn(
                "previous_year_value",
                F.lag("value", 12).over(
                    industrial_window
                ),
            )
            .withColumn(
                "value",
                (
                    (
                        F.col("value")
                        / F.col("previous_year_value")
                    )
                    - 1
                ) * 100,
            )
            .drop("previous_year_value")
            .withColumn(
                "symbol",
                F.lit(
                    "INDUSTRIAL_PRODUCTION_EU_YOY"
                ),
            )
        )

        # El resto de indicadores mensuales no se modifica.
        other_monthly_macro = (
            monthly_macro
            .filter(
                F.col("symbol")
                != "INDUSTRIAL_PRODUCTION_EU"
            )
        )

        # ============================================================
        # 6. UNIÓN DE VARIABLES MACRO
        # ============================================================

        macro_monthly_long = (
            daily_macro_monthly
            .unionByName(other_monthly_macro)
            .unionByName(industrial_production)
        )

        final_macro_symbols = [
            "VIX",
            "US_10Y",
            "FED_FUNDS",
            "CPI_US",
            "HICP_EU",
            "INDUSTRIAL_PRODUCTION_EU_YOY",
            "BUSINESS_SENTIMENT_EU",
        ]

        # ============================================================
        # 7. PIVOT: UNA FILA POR MES
        # ============================================================

        macro_monthly = (
            macro_monthly_long
            .groupBy("month_date")
            .pivot(
                "symbol",
                final_macro_symbols,
            )
            .agg(
                F.first("value")
            )
            .withColumnRenamed(
                "month_date",
                "date",
            )
        )

        # ============================================================
        # 8. UNIÓN MERCADO + MACRO
        # ============================================================

        result = (
            market_monthly
            .join(
                macro_monthly,
                on="date",
                how="left",
            )
        )

        # ============================================================
        # 9. COLUMNAS DE PARTICIÓN
        # ============================================================

        result = (
            result
            .withColumn(
                "year",
                F.year("date"),
            )
            .withColumn(
                "month",
                F.month("date"),
            )
        )

        return result.select(
            "date",
            "symbol",
            "adj_close",
            "market_return",
            "VIX",
            "US_10Y",
            "FED_FUNDS",
            "CPI_US",
            "HICP_EU",
            "INDUSTRIAL_PRODUCTION_EU_YOY",
            "BUSINESS_SENTIMENT_EU",
            "year",
            "month",
        )