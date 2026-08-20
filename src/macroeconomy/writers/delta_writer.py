from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from macroeconomy.utils.paths import get_layer_root, get_schemas


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
            "batch",
        )

        partition_cols = sink_config.get(
            "partitionBy",
            []
        )

        options = {
            "mergeSchema": "true",
            **sink_config.get("options", {}),
        }

        # ==========================================================
        # Batch
        # ==========================================================

        if run_mode == "batch":

            writer = (
                df.write
                .format("delta")
                .options(**options)
                .mode(
                    sink_config.get(
                        "mode",
                        "append",
                    )
                )
            )

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

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
                        "output_mode",
                        "append",
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