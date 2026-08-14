from pyspark.sql.streaming import StreamingQuery

from macroeconomy.readers.delta_reader import DeltaReader
from macroeconomy.utils.config import load_configs
from macroeconomy.utils.constants import SILVER
from macroeconomy.utils.transformers import get_etl
from macroeconomy.writers.delta_writer import DeltaWriter
from macroeconomy.utils.constants import TABLES


class SilverPipeline:

    def __init__(self, spark, config_file_name: str = "pipeline_config.yaml"):

        self.reader = DeltaReader(spark)
        self.writer = DeltaWriter(SILVER)
        self.pipeline_configs = load_configs(config_file_name)

    def run(
            self,
            datasource: str,
            dataset: str,
    ) -> StreamingQuery:

        bronze_table = TABLES[datasource][dataset]
        etl = get_etl(bronze_table)

        df = self.reader.read(datasource, dataset)

        transformed_df = etl.transform(
            df,
        )
        datasource_config = self.pipeline_configs[datasource]
        sink_config = datasource_config["sinks"][SILVER]

        return self.writer.write(
            transformed_df,
            sink_config,
            SILVER,
            datasource,
            dataset
        )