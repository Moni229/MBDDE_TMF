"""Writer de tablas Delta en modos batch y streaming"""

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from macroeconomy.utils.constants import (
    DEFAULT_OUTPUT_MODE,
    DEFAULT_STREAMING_TRIGGER_TIME,
    DELTA_FORMAT,
    RUN_MODE_AVAILABLE_NOW,
    RUN_MODE_BATCH,
    RUN_MODE_STREAMING,
    WRITE_MODE_OVERWRITE,
)


class DeltaWriter:
    """Persiste DataFrames en tablas Delta gestionadas"""

    def __init__(self, layer: str):
        self.layer = layer

    def write(
        self,
        df: DataFrame,
        sink_config: dict,
        target_table: str,
        target_path: str,
        query_name: str,
    ) -> StreamingQuery | None:
        run_mode = sink_config.get("run_mode", RUN_MODE_BATCH)
        partition_cols = sink_config.get("partitionBy", [])
        options = {
            "mergeSchema": "true",
            "path": target_path,
            **sink_config.get("options", {}),
        }

        if run_mode == RUN_MODE_BATCH:
            mode = sink_config.get("mode", DEFAULT_OUTPUT_MODE)

            writer = df.write.format(DELTA_FORMAT).options(**options).mode(mode)

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            if mode == WRITE_MODE_OVERWRITE and partition_cols:
                replace_where = self._infer_replace_where(
                    df=df,
                    partition_cols=partition_cols,
                )
                writer = writer.option("replaceWhere", replace_where)

            writer.saveAsTable(target_table)
            return None

        if run_mode == RUN_MODE_STREAMING:
            checkpoint = f"{target_path}/_checkpoint"

            writer = (
                df.writeStream.format(DELTA_FORMAT)
                .options(**options)
                .option("checkpointLocation", checkpoint)
                .queryName(query_name)
                .outputMode(sink_config.get("mode", DEFAULT_OUTPUT_MODE))
            )

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            writer = writer.trigger(processingTime=DEFAULT_STREAMING_TRIGGER_TIME)
            return writer.toTable(target_table)

        if run_mode == RUN_MODE_AVAILABLE_NOW:
            checkpoint = f"{target_path}/_checkpoint"

            writer = (
                df.writeStream.format(DELTA_FORMAT)
                .options(**options)
                .option("checkpointLocation", checkpoint)
                .queryName(query_name)
                .outputMode(sink_config.get("output_mode", DEFAULT_OUTPUT_MODE))
            )

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            writer = writer.trigger(availableNow=True)
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
        missing_columns = set(partition_cols) - set(df.columns)

        if missing_columns:
            raise ValueError(
                "Las columnas de partición no existen "
                f"en el DataFrame: {sorted(missing_columns)}"
            )

        partitions = df.select(*partition_cols).distinct().collect()

        if not partitions:
            raise ValueError(
                "No se pueden inferir las particiones: " "el DataFrame está vacío."
            )

        conditions = []

        for row in partitions:
            partition_conditions = []

            for column in partition_cols:
                value = row[column]

                if value is None:
                    condition = f"{column} IS NULL"
                elif isinstance(value, str):
                    escaped_value = value.replace("'", "''")
                    condition = f"{column} = '{escaped_value}'"
                else:
                    condition = f"{column} = {value}"

                partition_conditions.append(condition)

            conditions.append("(" + " AND ".join(partition_conditions) + ")")

        return " OR ".join(conditions)
