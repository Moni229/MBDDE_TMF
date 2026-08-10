from pyspark.sql import SparkSession, DataFrame

from macroeconomy.utils.paths import get_schemas

class DeltaReader:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schemas = get_schemas()

    def read(
            self,
            datasource: str,
            dataset: str,
    ) -> DataFrame:

        bronze_schema = self.schemas["bronze"]

        bronze_table = (
            f"{bronze_schema}.{datasource}_{dataset}"
        )

        return (
            self.spark.readStream
            .table(bronze_table)
        )