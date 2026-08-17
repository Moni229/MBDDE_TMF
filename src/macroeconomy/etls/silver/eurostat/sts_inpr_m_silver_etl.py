from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from macroeconomy.etls.etl_class import ETLClass


class StsInprMETL(ETLClass):

    def transform(self, df: DataFrame) -> DataFrame:

        # ==========================================================
        # 1. Obtener las dimensiones y sus tamaños desde el schema
        #
        # NO usamos .first() porque df es un streaming DataFrame.
        # ==========================================================

        dimensions = df.schema["id"].dataType.elementType

        # `id` es array<string>, por lo que los nombres concretos
        # de las dimensiones no están disponibles como valores del
        # schema.
        #
        # En este dataset conocemos el orden definido por JSON-stat:
        #
        # ["freq", "indic_bt", "nace_r2", "s_adj", "unit", "geo", "time"]
        #
        # El orden se obtiene del contenido de `id`, pero al ser
        # streaming no podemos leerlo con .first().
        #
        # Para STS_INPR_M el orden es fijo.
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
        #
        # STS_INPR_M:
        #
        # freq     -> 1
        # indic_bt -> 1
        # nace_r2  -> 1
        # s_adj    -> 1
        # unit     -> 1
        # geo      -> 1
        # time     -> 882
        #
        # No necesitamos leer `size` del DataFrame.
        # Lo obtenemos del schema de las categorías.
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
        #
        # El schema contiene:
        #
        # value:
        #     struct<
        #         456:double,
        #         457:double,
        #         ...
        #     >
        #
        # Creamos:
        #
        # {
        #     "456": valor,
        #     "457": valor,
        #     ...
        # }
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
            value_map
        )

        # ==========================================================
        # 4. Una fila por observación
        #
        # Explodeamos el MAP, no el STRUCT.
        #
        # _index -> 456, 457, 458...
        # value  -> valor de la observación
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
        # y:
        #
        # [1, 1, 1, 1, 1, 1, 882]
        #
        # time es la dimensión que cambia más rápidamente.
        # ==========================================================

        for i, dimension in enumerate(dimensions):

            # ------------------------------------------------------
            # Obtener los códigos de la dimensión desde el schema
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
            # Crear:
            #
            # posición -> código
            #
            # Ejemplo:
            #
            # freq:
            #     0 -> M
            #
            # time:
            #     0 -> 1953-01
            #     1 -> 1953-02
            #     ...
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
            # Recuperar código de dimensión
            # ------------------------------------------------------

            df = df.withColumn(
                dimension,
                F.element_at(
                    inverse_map,
                    dimension_position.cast("string"),
                )
            )

        # ==========================================================
        # 6. Seleccionar columnas
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
        # Se convierte en:
        #
        # 1953-01-01
        # 1953-02-01
        # ...
        # ==========================================================

        df = df.withColumn(
            "observation_date",
            F.to_date(
                F.concat(
                    F.col("time"),
                    F.lit("-01"),
                )
            )
        )

        # ==========================================================
        # 8. Columnas de partición
        #
        # Corresponden a la fecha del dato.
        # ==========================================================

        df = (
            df
            .withColumn(
                "year",
                F.year("observation_date"),
            )
            .withColumn(
                "month",
                F.month("observation_date"),
            )
            .withColumn(
                "day",
                F.dayofmonth("observation_date"),
            )
        )

        # ==========================================================
        # 9. Resultado final
        # ==========================================================

        return df.select(
            "freq",
            "indic_bt",
            "nace_r2",
            "s_adj",
            "unit",
            "geo",
            "time",
            "observation_date",
            "value",
            "year",
            "month",
            "day",
            "_ingested_at",
            "_source_file",
        )