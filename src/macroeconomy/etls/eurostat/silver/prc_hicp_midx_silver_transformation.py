from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.transformer.silver_transformer import SilverTransformer


class EurostatSilverTransformer(SilverTransformer):

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transforma una respuesta JSON-stat 2.0 de Eurostat
        en una fila por observación.

        Resultado:

            date
            value
            year
            month
            day
            _ingested_at
            _source_file
        """

        # ------------------------------------------------------------
        # 1. Convertimos `value` (STRUCT) en MAP
        # ------------------------------------------------------------

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

        # ------------------------------------------------------------
        # 2. Una fila por valor
        # ------------------------------------------------------------

        df = df.select(
            F.explode("_values").alias("_index", "value"),
            "dimension",
            "size",
            "_ingested_at",
            "_source_file",
        )

        # ------------------------------------------------------------
        # 3. `time` es la última dimensión:
        #
        # id = ["freq", "unit", "coicop", "geo", "time"]
        #
        # Por tanto, el índice temporal es:
        #
        #     index % size_of_time
        # ------------------------------------------------------------

        time_size = F.element_at(
            F.col("size"),
            -1,
        )

        df = df.withColumn(
            "_time_position",
            F.pmod(
                F.col("_index").cast("long"),
                time_size,
            ),
        )

        # ------------------------------------------------------------
        # 4. `dimension.time.category.index` es STRUCT.
        #
        # Convertimos:
        #
        #     1996-01 -> 0
        #     1996-02 -> 1
        #     ...
        #
        # en:
        #
        #     0 -> 1996-01
        #     1 -> 1996-02
        #     ...
        # ------------------------------------------------------------

        time_index_fields = (
            df.schema["dimension"]
            .dataType["time"]
            .dataType["category"]
            .dataType["index"]
            .dataType
            .fieldNames()
        )

        time_map = F.create_map(
            *[
                item
                for position, field in enumerate(time_index_fields)
                for item in (
                    F.lit(position),
                    F.lit(field),
                )
            ]
        )

        # ------------------------------------------------------------
        # 5. Recuperamos la fecha
        # ------------------------------------------------------------

        df = df.withColumn(
            "date",
            F.element_at(
                time_map,
                F.col("_time_position"),
            ),
        )

        # ------------------------------------------------------------
        # 6. Convertimos YYYY-MM a fecha
        # ------------------------------------------------------------

        df = df.withColumn(
            "date",
            F.to_date(
                F.concat(
                    F.col("date"),
                    F.lit("-01"),
                )
            ),
        )

        # ------------------------------------------------------------
        # 7. Generamos las columnas de partición
        #
        # Estas corresponden a la fecha del dato, NO a la ingesta.
        # ------------------------------------------------------------

        df = (
            df
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
        )

        # ------------------------------------------------------------
        # 8. Resultado final
        # ------------------------------------------------------------

        return df.select(
            "date",
            F.col("value").cast("double").alias("value"),
            "year",
            "month",
            "day",
            "_ingested_at",
            "_source_file",
        )