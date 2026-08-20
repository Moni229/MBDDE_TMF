from macroeconomy.writers.delta_writer import DeltaWriter

from macroeconomy.utils.config import load_configs
from macroeconomy.utils.constants import GOLD
from macroeconomy.utils.transformers import get_etl
from macroeconomy.utils.paths import get_schemas, get_layer_root

from macroeconomy.readers.delta_reader import DeltaReader


class GoldPipeline:

    def __init__(
        self,
        spark,
        config_file_name: str = "gold_config.yaml"
    ):
        self.layer = GOLD
        self.spark = spark
        self.reader = DeltaReader(spark)
        self.writer = DeltaWriter(GOLD)
        self.gold_configs = load_configs(config_file_name)

    def run(self, etl_name: str) -> None:

        etl = get_etl(GOLD, etl_name)

        etl_configs = self.gold_configs[etl_name]

        source_etl_config = etl_configs["source"]
        options = etl_configs.get("options", {})
        sink_etl_config = etl_configs["sink"]
        run_mode = sink_etl_config.get(
            "run_mode",
            "batch",
        )

        print(f"Processing Gold with run_mode {run_mode}")

        # ==========================================================
        # 1. Read sources
        # ==========================================================

        source_dfs = {}

        for source_table in source_etl_config:

            print(f"Reading source: {source_table}")

            source_dfs[source_table] = self.reader.read(
                layer = self.layer,
                table = source_table,
                run_mode = run_mode
            )

        # ==========================================================
        # 2. Transform
        # ==========================================================

        gold_df = etl.transform(
            source_dfs,
            options
        )

        print("=== GOLD DF ===")
        gold_df.printSchema()

        # ==========================================================
        # 3. Write
        # ==========================================================

        table_name = sink_etl_config["target_table"]

        target_table = (
            f"{get_schemas()[GOLD]}.{table_name}"
        )

        target_path = (
            f"{get_layer_root(GOLD)}/{etl_name}"
        )

        query_name = f"{GOLD}-{etl_name}"

        self.writer.write(
            gold_df,
            sink_etl_config,
            target_table,
            target_path,
            query_name
        )