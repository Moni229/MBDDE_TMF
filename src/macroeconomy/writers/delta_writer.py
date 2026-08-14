from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from macroeconomy.utils.paths import get_layer_root, get_schemas

from macroeconomy.utils.constants import TABLES


class DeltaWriter:

    def __init__(self, layer: str):
        self.schemas = get_schemas()
        self.layer_root = get_layer_root(layer)

    def write(
            self,
            df: DataFrame,
            sink_config: dict,
            layer: str,
            datasource: str,
            dataset: str
    ) -> StreamingQuery:

        target_table = (
            f"{self.schemas[layer]}.{TABLES[datasource][dataset]}"
        )

        bronze_path = (
            f"{self.layer_root}/{datasource}/{dataset}"
        )

        checkpoint = (
            f"{bronze_path}/_checkpoint"
        )

        query_name = (
            f"{layer}-{datasource}-{dataset}"
        )

        partition_cols = sink_config.get("partitionBy", [])

        options = {
            "mergeSchema": "true",
            **sink_config.get("options", {}),
        }

        writer = (
            df.writeStream
            .format("delta")
            .options(**options)
            .option(
                "checkpointLocation",
                checkpoint,
            )
            .queryName(query_name)
            .outputMode("append")
        )

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        run_mode = sink_config.get(
            "run_mode",
            "batch",
        )

        if run_mode == "streaming":
            writer = writer.trigger(
                processingTime="10 seconds"
            )
        else:
            writer = writer.trigger(
                availableNow=True
            )

        return writer.toTable(target_table)