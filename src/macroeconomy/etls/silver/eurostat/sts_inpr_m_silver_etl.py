from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class StsInprMETL(ETLClass):

    def transform(self, df: DataFrame) -> DataFrame:

        # ==========================================================
        # 1. Dimensiones del dataset
        #
        # Orden definido por JSON-stat:
        #
        # ["freq", "indic_bt", "nace_r2", "s_adj", "unit", "geo", "time"]
        # ==========================================================

        dimensions = [
            "freq",
            "indic_bt",
            "nace_r2",
            "s_adj",
            "unit",
            "geo",
            "time",
        ]

        # ==========================================================
        # 2. Tamaños de las dimensiones
        # ==========================================================

        dimension_sizes = []

        for dimension in dimensions:

            index_type = (
                df.schema["dimension"]
                .dataType[dimension]
                .dataType["category"]
                .dataType["index"]
                .dataType
            )

            dimension_sizes.append(
                len(index_type.fieldNames())
            )

        # ==========================================================
        # 3. Convertir `value` STRUCT en MAP
        # ==========================================================

        value_fields = (
            df.schema["value"]
            .dataType
            .fieldNames()
        )

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

        df = df.withColumn(
            "_values",
            value_map,
        )

        # ==========================================================
        # 4. Una fila por observación
        # ==========================================================

        df = df.select(
            F.explode("_values").alias(
                "_index",
                "value",
            ),
            "dimension",
            "_ingested_at",
            "_source_file",
        )

        # ==========================================================
        # 5. Reconstruir las dimensiones
        #
        # JSON-stat utiliza un orden multidimensional.
        #
        # En este dataset:
        #
        # [freq, indic_bt, nace_r2, s_adj, unit, geo, time]
        #
        # time es la dimensión que cambia más rápidamente.
        # ==========================================================

        for i, dimension in enumerate(dimensions):

            # ------------------------------------------------------
            # Códigos de la dimensión
            # ------------------------------------------------------

            index_fields = (
                df.schema["dimension"]
                .dataType[dimension]
                .dataType["category"]
                .dataType["index"]
                .dataType
                .fieldNames()
            )

            # ------------------------------------------------------
            # Crear mapa:
            #
            # posición -> código
            # ------------------------------------------------------

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

            # ------------------------------------------------------
            # Producto de los tamaños de las dimensiones posteriores
            # ------------------------------------------------------

            multiplier = 1

            for size in dimension_sizes[i + 1:]:
                multiplier *= size

            current_size = dimension_sizes[i]

            # ------------------------------------------------------
            # Posición de la dimensión dentro del cubo
            # ------------------------------------------------------

            dimension_position = (
                F.floor(
                    F.col("_index").cast("long")
                    / F.lit(multiplier)
                )
                % F.lit(current_size)
            )

            # ------------------------------------------------------
            # Recuperar código
            # ------------------------------------------------------

            df = df.withColumn(
                dimension,
                F.element_at(
                    inverse_map,
                    dimension_position.cast("string"),
                ),
            )

        # ==========================================================
        # 6. Seleccionar dimensiones y metadatos
        # ==========================================================

        df = df.select(
            *dimensions,
            F.col("value").cast("double").alias("value"),
            "_ingested_at",
            "_source_file",
        )

        # ==========================================================
        # 7. Crear fecha de observación
        #
        # time:
        #
        # 1953-01
        # 1953-02
        # ...
        #
        # se convierte en:
        #
        # 1953-01-01
        # 1953-02-01
        # ...
        # ==========================================================

        df = df.withColumn(
            "date",
            F.to_date(
                F.concat(
                    F.col("time"),
                    F.lit("-01"),
                ),
                "yyyy-MM-dd",
            ),
        )

        # ==========================================================
        # 8. Columnas de partición
        #
        # Corresponden a la fecha del dato,
        # NO a la fecha de ingesta.
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
        # 9. Resultado final
        # ==========================================================

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