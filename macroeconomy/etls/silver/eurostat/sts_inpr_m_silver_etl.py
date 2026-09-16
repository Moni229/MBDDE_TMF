from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class StsInprMETL(ETLClass):
    """Normaliza los datos de producción industrial de Eurostat en filas de Silver"""

    def transform(self, df: DataFrame) -> DataFrame:
        dimensions = [
            "freq",
            "indic_bt",
            "nace_r2",
            "s_adj",
            "unit",
            "geo",
            "time",
        ]

        dimension_sizes = []

        for dimension in dimensions:
            index_type = (
                df.schema["dimension"]
                .dataType[dimension]
                .dataType["category"]
                .dataType["index"]
                .dataType
            )
            dimension_sizes.append(len(index_type.fieldNames()))

        value_fields = df.schema["value"].dataType.fieldNames()
        value_map = F.create_map(
            *[
                item
                for field in value_fields
                for item in (
                    F.lit(field),
                    F.col(f"value.`{field}`"),
                )
            ]
        )
        df = df.withColumn("_values", value_map)

        df = df.select(
            F.explode("_values").alias("_index", "value"),
            "dimension",
            "_ingested_at",
            "_source_file",
        )

        for i, dimension in enumerate(dimensions):
            index_fields = (
                df.schema["dimension"]
                .dataType[dimension]
                .dataType["category"]
                .dataType["index"]
                .dataType.fieldNames()
            )

            inverse_map = F.create_map(
                *[
                    item
                    for position, field in enumerate(index_fields)
                    for item in (
                        F.lit(str(position)),
                        F.lit(field),
                    )
                ]
            )

            multiplier = 1
            for size in dimension_sizes[i + 1 :]:
                multiplier *= size

            current_size = dimension_sizes[i]
            dimension_position = F.floor(
                F.col("_index").cast("long") / F.lit(multiplier)
            ) % F.lit(current_size)

            df = df.withColumn(
                dimension,
                F.element_at(
                    inverse_map,
                    dimension_position.cast("string"),
                ),
            )

        df = df.select(
            *dimensions,
            F.col("value").cast("double").alias("value"),
            "_ingested_at",
            "_source_file",
        )

        df = df.withColumn(
            "date",
            F.to_date(
                F.concat(F.col("time"), F.lit("-01")),
                "yyyy-MM-dd",
            ),
        )

        df = (
            df.withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
        )

        return df.select(
            F.lit("monthly").alias("frequency"),
            "indic_bt",
            "nace_r2",
            "s_adj",
            "unit",
            "geo",
            "time",
            "date",
            "value",
            "year",
            "month",
            "day",
            "_ingested_at",
            "_source_file",
        )
