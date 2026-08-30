from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class PrcHipcManrETL(ETLClass):

    def transform(self, df: DataFrame) -> DataFrame:

        # ==========================================================
        # 1. Convertimos los STRUCT dinámicos de Eurostat a MAP
        # ==========================================================

        df = (
            df
            .withColumn(
                "time_index",
                F.from_json(
                    F.to_json("dimension.time.category.index"),
                    "map<string,bigint>",
                ),
            )
            .withColumn(
                "values",
                F.from_json(
                    F.to_json("value"),
                    "map<string,double>",
                ),
            )
        )

        # ==========================================================
        # 2. Creamos el mapa:
        #
        #     posición -> periodo
        #
        #     0 -> 1996-01
        #     1 -> 1996-02
        #     ...
        # ==========================================================

        df = df.withColumn(
            "time_by_index",
            F.map_from_entries(
                F.transform(
                    F.map_entries("time_index"),
                    lambda x: F.struct(
                        x["value"].alias("key"),
                        x["key"].alias("value"),
                    ),
                )
            ),
        )

        # ==========================================================
        # 3. Una fila por observación
        # ==========================================================

        df = (
            df
            .select(
                F.col(
                    "dimension.freq.category.label.M"
                ).alias("frequency"),

                F.col(
                    "dimension.geo.category.label.EA20"
                ).alias("geo"),

                F.col(
                    "dimension.coicop.category.label"
                ).alias("coicop"),

                F.col(
                    "dimension.unit.category.label"
                ).alias("unit"),

                F.explode("values").alias(
                    "position",
                    "value",
                ),

                "time_by_index",

                F.col("updated"),
                F.col("id"),
                F.col("source"),
                F.col("version"),
                F.col("_ingested_at"),
                F.col("_source_file"),
            )
        )

        # ==========================================================
        # 4. Recuperamos el periodo
        # ==========================================================

        df = df.withColumn(
            "date",
            F.element_at(
                F.col("time_by_index"),
                F.col("position").cast("bigint"),
            ),
        )

        # ==========================================================
        # 5. Convertimos YYYY-MM a fecha
        # ==========================================================

        df = df.withColumn(
            "date",
            F.to_date(
                F.concat(
                    F.col("date"),
                    F.lit("-01"),
                ),
                "yyyy-MM-dd",
            ),
        )

        # ==========================================================
        # 6. Columnas de partición
        # ==========================================================

        df = (
            df
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

        # ==========================================================
        # 7. Resultado final
        # ==========================================================

        return df.select(
            "date",
            F.col("value").cast("double").alias("value"),
            F.lower("frequency").alias("frequency"),
            "geo",
            "coicop",
            "unit",
            "updated",
            "id",
            "source",
            "version",
            "year",
            "month",
            "day",
            "_ingested_at",
            "_source_file",
        )