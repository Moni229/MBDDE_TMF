class BronzePipeline:

    def __init__(
        self,
        reader,
        writer,
    ):
        self.reader = reader
        self.writer = writer

    def run(self, source, source_config: dict, dataset: str | None = None, schema=None, kafka_config: dict | None = None):

        # Seleccionar un único dataset: el indicado o el primero de la lista
        dataset_name = dataset if dataset else source_config.get("datasets", [None])[0]
        if not dataset_name:
            raise ValueError("No dataset specified and source_config contains no datasets")

        print(f"Processing {getattr(source, 'config_key', str(source))} - {dataset_name}")

        # Construir configuración de ingestión para este dataset
        ingestion_config = {**source_config, "dataset": dataset_name}

        # Leer datos
        df = self.reader.read(
            ingestion_config,
            source,
            dataset_name
        )

        self.writer.write(df, dataset_name, ingestion_config)
