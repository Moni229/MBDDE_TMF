from macroeconomy.sources.datasource import DataSource

from macroeconomy.writers.landing_writer import LandingWriter


class LandingPipeline:

    def __init__(self, writer: LandingWriter):
        self.writer = writer

    def run(self, source: DataSource, source_config: dict, dataset: str | None = None, ) -> None:
        dataset_names = [dataset] if dataset else source_config["datasets"]

        source_params = {
            k: v
            for k, v in source_config.items()
            if k not in ("source", "datasets")
        }

        for dataset_name in dataset_names:

            print(f"Processing {source.config_key} - {dataset_name}")


            data = source.read(
                dataset=dataset_name,
                **source_params,
            )

            self.writer.write(
                data=data,
                source=source.config_key,
                dataset=dataset_name
            )