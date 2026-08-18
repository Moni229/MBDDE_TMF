from macroeconomy.readers.ingestion_reader import IngestionReader
from macroeconomy.writers.delta_writer import DeltaWriter

from macroeconomy.utils.config import load_configs

from macroeconomy.utils.constants import BRONZE

from macroeconomy.utils.constants import TABLES
from macroeconomy.utils.paths import get_schemas, get_layer_root


class BronzePipeline:

    def __init__(
            self,
            reader: IngestionReader,
            writer: DeltaWriter,
            config_file_name: str = "pipeline_config.yaml",
    ):
        self.reader = reader
        self.writer = writer
        self.pipeline_configs = load_configs(config_file_name)
        self.layer_root = BRONZE

    def run(self, datasource: str, dataset: str | None = None, schema=None):

        datasource_config = self.pipeline_configs[datasource]
        # Seleccionar un único dataset: el indicado o el primero de la lista
        dataset_name = dataset if dataset else datasource_config.get("datasets", [None])[0]
        if not dataset_name:
            raise ValueError("No dataset specified and datasource_config contains no datasets")

        source_config: dict = datasource_config["source"]
        sink_config = datasource_config["sinks"][self.layer_root]

        print(f"Processing {getattr(datasource, 'config_key', datasource)} - {dataset_name}")

        # Leer datos
        df = self.reader.read(
            datasource,
            dataset_name,
            source_config,
        )
        print("=== BRONZE DF ===")
        print(df.columns)

        table_name = TABLES[datasource][dataset]

        target_table = (
            f"{get_schemas()[self.layer_root]}.{table_name}"
        )

        target_path = (
            f"{get_layer_root(self.layer_root)}/{datasource}/{dataset}"
        )
        query_name = (
            f"{self.layer_root}-{datasource}-{dataset}"
        )
        query = self.writer.write(df, sink_config, target_table, target_path, query_name)
        return query
