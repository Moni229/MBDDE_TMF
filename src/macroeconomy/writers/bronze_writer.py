from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from macroeconomy.utils.paths import get_bronze_root, get_schemas


class BronzeWriter:

    def __init__(self):
        self.schemas = get_schemas()
        self.bronze_root = get_bronze_root()

    def write(
            self,
            df: DataFrame,
            dataset: str,
            ingestion_config: dict,
    ) -> StreamingQuery:

        datasource = ingestion_config["datasource"]

        sink = ingestion_config["sink"]

        layer = sink["layer"]

        target_table = (
            f"{self.schemas[layer]}.{datasource}_{dataset}"
        )

        bronze_path = (
            f"{self.bronze_root}/{datasource}/{dataset}"
        )

        checkpoint = (
            f"{bronze_path}/_checkpoint"
        )

        query_name = (
            f"bronze-{datasource}-{dataset}"
        )

        partition_cols = sink.get("partitionBy", [])

        options = {
            "mergeSchema": "true",
            **sink.get("options", {}),
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

        run_mode = ingestion_config.get(
            "run_mode",
            "batch",
        )

        if run_mode == "streaming":
            writer = writer.trigger(
                processingTime="60 seconds"
            )
        else:
            writer = writer.trigger(
                availableNow=True
            )

        return writer.toTable(target_table)