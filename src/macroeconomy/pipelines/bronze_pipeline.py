from macroeconomy.readers.ingestion_reader import IngestionReader
from macroeconomy.writers.bronze_writer import BronzeWriter

from macroeconomy.utils.config import load_configs


class BronzePipeline:

    def __init__(
            self,
            reader: IngestionReader,
            writer: BronzeWriter,
            config_file_name: str = "bronze_config.yaml",
    ):
        self.reader = reader
        self.writer = writer
        self.bronze_configs = load_configs(config_file_name)

    def run(self, source: str, dataset: str | None = None, schema=None, kafka_config: dict | None = None):

        bronze_config = self.bronze_configs[source]
        # Seleccionar un único dataset: el indicado o el primero de la lista
        dataset_name = dataset if dataset else bronze_config.get("datasets", [None])[0]
        if not dataset_name:
            raise ValueError("No dataset specified and source_config contains no datasets")

        print(f"Processing {getattr(source, 'config_key', source)} - {dataset_name}")

        # Construir configuración de ingestión para este dataset
        ingestion_config = {**bronze_config, "datasource": source, "dataset": dataset_name}

        # Leer datos
        df = self.reader.read(
            ingestion_config
        )
        print("=== BRONZE DF ===")
        print(df.columns)
        df.printSchema()
        self.writer.write(df, ingestion_config)
