from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class PrcHipcManrETL(ETLClass):
    """Normaliza la serie mensual HICP de Eurostat en filas de Silver"""

    def transform(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "time_index",
            F.from_json(
                F.to_json("dimension.time.category.index"),
                "map<string,bigint>",
            ),
        ).withColumn(
            "values",
            F.from_json(
                F.to_json("value"),
                "map<string,double>",
            ),
        )

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

        df = df.select(
            F.col("dimension.freq.category.label.M").alias("frequency"),
            F.col("dimension.geo.category.label.EA20").alias("geo"),
            F.col("dimension.coicop.category.label").alias("coicop"),
            F.col("dimension.unit.category.label").alias("unit"),
            F.explode("values").alias("position", "value"),
            "time_by_index",
            F.col("updated"),
            F.col("id"),
            F.col("source"),
            F.col("version"),
            F.col("_ingested_at"),
            F.col("_source_file"),
        )

        df = df.withColumn(
            "date",
            F.element_at(
                F.col("time_by_index"),
                F.col("position").cast("bigint"),
            ),
        )

        df = df.withColumn(
            "date",
            F.to_date(
                F.concat(F.col("date"), F.lit("-01")),
                "yyyy-MM-dd",
            ),
        )

        df = (
            df.withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
        )

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
