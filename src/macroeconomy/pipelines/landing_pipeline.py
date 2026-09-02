"""Orquestación del pipeline de landing"""

from pyspark.sql.connect.session import SparkSession

from macroeconomy.sources.datasource import DataSource
from macroeconomy.utils.config import load_configs
from macroeconomy.utils.constants import CONFIG_INGESTION_FILE
from macroeconomy.writers.landing_writer import LandingWriter


class LandingPipeline:
    """Lee de una fuente de datos y los persiste en landing"""

    def __init__(
        self,
        spark: SparkSession,
        config_file_name: str = CONFIG_INGESTION_FILE,
    ):
        self.writer = LandingWriter(spark)
        self.ingestion_configs = load_configs(config_file_name)

    def run(
        self,
        source: DataSource,
        source_name: str,
        dataset: str | None = None,
    ) -> None:
        source_config = self.ingestion_configs[source_name]
        datasets_config = source_config["datasets"]

        if dataset:
            datasets_config = [d for d in datasets_config if d["name"] == dataset]

        source_params = {
            k: v for k, v in source_config.items() if k not in ("source", "datasets")
        }

        for dataset_config in datasets_config:
            dataset_name = dataset_config["name"]
            dataset_params = dataset_config.get("params", {})

            params = {
                **source_params,
                **dataset_params,
            }

            print(f"Processing {source.config_key} - {dataset_name}")

            if source.is_streaming:
                self._run_streaming(
                    source=source,
                    dataset=dataset_name,
                    source_params=params,
                )
            else:
                self._run_batch(
                    source=source,
                    dataset=dataset_name,
                    source_params=params,
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
