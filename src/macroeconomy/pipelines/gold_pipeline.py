from macroeconomy.etl.dim_asset_etl import DimAssetETL
from macroeconomy.writers.delta_writer import DeltaWriter

from macroeconomy.utils.config import load_configs
from macroeconomy.utils.constants import GOLD

from macroeconomy.utils.transformers import get_etl

from macroeconomy.utils.paths import get_schemas, get_layer_root


class GoldPipeline:

    def __init__(self, config_file_name: str = "gold_config.yaml"):

        self.writer = DeltaWriter(GOLD)
        self.gold_configs = load_configs(config_file_name)

    def run(self, etl_name: str) -> None:

        etl = get_etl(GOLD, etl_name)
        etl_configs = self.gold_configs[etl_name]
        source_etl_config = etl_configs["source"]
        sink_etl_config = etl_configs["sink"]

        print("Processing Gold")

        # ==========================================================
        # 1. Transform
        # ==========================================================
        gold_df = etl.transform(source_etl_config)

        print("=== GOLD DF ===")
        gold_df.printSchema()
        gold_df.show(truncate=False)

        # ==========================================================
        # 2. Write
        # ==========================================================
        table_name = sink_etl_config["target_table"]

        target_table = (
            f"{get_schemas()[GOLD]}.{table_name}"
        )

        target_path = (
            f"{get_layer_root(GOLD)}/{etl_name}"
        )
        query_name = (
            f"{GOLD}-{etl_name}"
        )
        self.writer.write(
            gold_df,
            sink_etl_config,
            GOLD,
            target_table,
            target_path,
            query_name
        )