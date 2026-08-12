from macroeconomy.sources.datasource import DataSource
from macroeconomy.writers.landing_writer import LandingWriter
from macroeconomy.utils.config import load_configs


class LandingPipeline:

    def __init__(self, writer: LandingWriter, config_file_name: str = "ingestion_config.yaml"):
        self.writer = writer
        self.ingestion_configs = load_configs(config_file_name)

    def run(self, source: DataSource, source_name: str, dataset: str | None = None, ) -> None:
        source_config = self.ingestion_configs[source_name]
        dataset_names = [dataset] if dataset else source_config["datasets"]

        source_params = {
            k: v
            for k, v in source_config.items()
            if k not in ("source", "datasets")
        }

        for dataset_name in dataset_names:

            print(f"Processing {source.config_key} - {dataset_name}")

            if source.is_streaming:
                self._run_streaming(
                    source=source,
                    dataset=dataset_name,
                    source_params=source_params,
                )

            else:
                self._run_batch(
                    source=source,
                    dataset=dataset_name,
                    source_params=source_params,
                )

    def _run_batch(
        self,
        source: DataSource,
        dataset: str,
        source_params: dict,
    ) -> None:

        data = source.read(
            dataset=dataset,
            **source_params,
        )

        self.writer.write(
            data=data,
            source=source.config_key,
            dataset=dataset,
        )

    def _run_streaming(
        self,
        source: DataSource,
        dataset: str,
        source_params: dict,
    ) -> None:

        def on_data(data):
            self.writer.write(
                data=data,
                source=source.config_key,
                dataset=dataset,
            )

        source.read(
            dataset=dataset,
            on_data=on_data,
            **source_params,
        )