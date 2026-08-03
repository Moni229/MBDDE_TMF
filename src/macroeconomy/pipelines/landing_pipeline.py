from macroeconomy.sources.datasource import DataSource


class LandingPipeline:

    def __init__(self, writer):
        self.writer = writer

    def run(self, source: DataSource, config, dataset=None):

        datasets = (
            [dataset]
            if dataset
            else config["datasets"]
        )

        extra_params = {
            k: v
            for k, v in config.items()
            if k != "datasets"
        }

        for ds in datasets:

            print(f"Processing {source.config_key} - {ds}")

            data = source.read(
                ds,
                **extra_params
            )

            self.writer.write(
                data=data,
                source=source.config_key,
                dataset=ds
            )