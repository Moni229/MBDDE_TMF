"""Orquestación del pipeline de Bronze."""

from pyspark.sql import SparkSession

from macroeconomy.readers.ingestion_reader import IngestionReader
from macroeconomy.utils.config import load_configs
from macroeconomy.utils.constants import (
    BRONZE,
    CONFIG_PIPELINE_FILE,
    TABLES,
)
from macroeconomy.utils.paths import get_layer_root, get_schemas
from macroeconomy.writers.delta_writer import DeltaWriter


class BronzePipeline:
    """Ingesta los datos de landing en tablas Delta de Bronze."""

    def __init__(
        self,
        spark: SparkSession,
        config_file_name: str = CONFIG_PIPELINE_FILE,
    ):
        self.reader = IngestionReader(spark)
        self.layer_root = BRONZE
        self.writer = DeltaWriter(self.layer_root)
        self.pipeline_configs = load_configs(config_file_name)

    def run(self, datasource: str, dataset: str | None = None, schema=None):
        datasource_config = self.pipeline_configs[datasource]
        dataset_name = dataset or datasource_config.get("datasets", [None])[0]

        if not dataset_name:
            raise ValueError(
                "No se indicó ningún dataset y datasource_config no contiene datasets"
            )

        source_config: dict = datasource_config["source"]
        sink_config = datasource_config["sinks"][self.layer_root]

        print(f"Processing {datasource} - {dataset_name}")

        df = self.reader.read(
            datasource,
            dataset_name,
            source_config,
        )
        print("=== BRONZE DF ===")
        print(df.columns)

        table_name = TABLES[datasource][dataset_name]
        target_table = f"{get_schemas()[self.layer_root]}.{table_name}"
        target_path = f"{get_layer_root(self.layer_root)}/{datasource}/{dataset_name}"
        query_name = f"{self.layer_root}-{datasource}-{dataset_name}"

        query = self.writer.write(
            df, sink_config, target_table, target_path, query_name
        )
        print("=== BRONZE DF END ===")
        return query
