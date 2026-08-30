from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from macroeconomy.utils.paths import get_layer_root, get_schemas

from macroeconomy.utils.constants import DEFAULT_MODE, DEFAULT_RUN_MODE


class DeltaWriter:

    def __init__(self, layer: str):
        self.layer = layer
        self.schemas = get_schemas()
        self.layer_root = get_layer_root(layer)

    def write(
        self,
        df: DataFrame,
        sink_config: dict,
        target_table: str,
        target_path: str,
        query_name: str,
    ) -> StreamingQuery | None:

        run_mode = sink_config.get(
            "run_mode",
            DEFAULT_RUN_MODE,
        )

        partition_cols = sink_config.get(
            "partitionBy",
            []
        )

        options = {
            "mergeSchema": "true",
            "path": target_path,
            **sink_config.get("options", {}),
        }

        # ==========================================================
        # Batch
        # ==========================================================

        if run_mode == "batch":

            mode = sink_config.get(
                "mode",
                DEFAULT_MODE,
            )

            writer = (
                df.write
                .format("delta")
                .options(**options)
                .mode(mode)
            )

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            if mode == "overwrite" and partition_cols:

                replace_where = self._infer_replace_where(
                    df=df,
                    partition_cols=partition_cols,
                )

                writer = writer.option(
                    "replaceWhere",
                    replace_where,
                )

            writer.saveAsTable(target_table)

            return None

        # ==========================================================
        # Streaming
        # ==========================================================

        if run_mode == "streaming":

            checkpoint = f"{target_path}/_checkpoint"

            writer = (
                df.writeStream
                .format("delta")
                .options(**options)
                .option(
                    "checkpointLocation",
                    checkpoint,
                )
                .queryName(query_name)
                .outputMode(
                    sink_config.get(
                        "mode",
                        DEFAULT_MODE,
                    )
                )
            )

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            writer = writer.trigger(
                processingTime="10 seconds"
            )

            return writer.toTable(target_table)

        # ==========================================================
        # Streaming + availableNow
        # ==========================================================

        if run_mode == "available_now":

            checkpoint = f"{target_path}/_checkpoint"

            writer = (
                df.writeStream
                .format("delta")
                .options(**options)
                .option(
                    "checkpointLocation",
                    checkpoint,
                )
                .queryName(query_name)
                .outputMode(
                    sink_config.get(
                        "output_mode",
                        "append",
                    )
                )
            )

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            writer = writer.trigger(
                availableNow=True
            )

            return writer.toTable(target_table)

        raise ValueError(
            f"Unsupported run_mode '{run_mode}'. "
            "Expected: 'batch', 'streaming' or 'available_now'."
        )

    @staticmethod
    def _infer_replace_where(
        df: DataFrame,
        partition_cols: list[str],
    ) -> str:

        # ----------------------------------------------------------
        # Comprobar que las columnas existen
        # ----------------------------------------------------------

        missing_columns = (
            set(partition_cols) - set(df.columns)
        )

        if missing_columns:
            raise ValueError(
                "Las columnas de partición no existen "
                f"en el DataFrame: {sorted(missing_columns)}"
            )

        # ----------------------------------------------------------
        # Obtener las combinaciones únicas de particiones
        # ----------------------------------------------------------

        partitions = (
            df
            .select(*partition_cols)
            .distinct()
            .collect()
        )

        if not partitions:
            raise ValueError(
                "No se pueden inferir las particiones: "
                "el DataFrame está vacío."
            )

        # ----------------------------------------------------------
        # Construir replaceWhere
        # ----------------------------------------------------------

        conditions = []

        for row in partitions:

            partition_conditions = []

            for column in partition_cols:

                value = row[column]

                if value is None:

                    condition = (
                        f"{column} IS NULL"
                    )

                elif isinstance(value, str):

                    escaped_value = (
                        value.replace("'", "''")
                    )

                    condition = (
                        f"{column} = '{escaped_value}'"
                    )

                else:

                    condition = (
                        f"{column} = {value}"
                    )

                partition_conditions.append(
                    condition
                )

            conditions.append(
                "("
                + " AND ".join(partition_conditions)
                + ")"
            )

        return " OR ".join(conditions)