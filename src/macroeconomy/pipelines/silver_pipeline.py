from pyspark.sql.streaming import StreamingQuery

from macroeconomy.readers.delta_reader import DeltaReader
from macroeconomy.utils.config import load_configs
from macroeconomy.utils.constants import SILVER, BRONZE, TABLES
from macroeconomy.utils.transformers import get_etl
from macroeconomy.writers.delta_writer import DeltaWriter
from macroeconomy.utils.paths import get_schemas, get_layer_root


class SilverPipeline:

    def __init__(self, spark, config_file_name: str = "pipeline_config.yaml"):

        self.reader = DeltaReader(spark)
        self.writer = DeltaWriter(SILVER)
        self.pipeline_configs = load_configs(config_file_name)
        self.layer = SILVER

    def run(
        self,
        datasource: str,
        dataset: str,
        partitions: list[dict] = None,
    ) -> StreamingQuery:

        bronze_table = TABLES[datasource][dataset]
        etl = get_etl(self.layer, bronze_table)

        datasource_config = self.pipeline_configs[datasource]
        sink_config = datasource_config["sinks"][self.layer]
        run_mode = sink_config.get(
            "run_mode",
            "batch",
        )

        df = self.reader.read(
            layer=BRONZE,
            datasource=datasource,
            dataset=dataset,
            run_mode=run_mode,
            partitions=partitions
        )

        transformed_df = etl.transform(
            df,
        )
        table_name = TABLES[datasource][dataset]

        target_table = (
            f"{get_schemas()[self.layer]}.{table_name}"
        )

        target_path = (
            f"{get_layer_root(self.layer)}/{datasource}/{dataset}"
        )
        query_name = (
            f"{self.layer}-{datasource}-{dataset}"
        )
        return self.writer.write(
            transformed_df,
            sink_config,
            target_table,
            target_path,
            query_name,
        )