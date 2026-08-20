from pyspark.sql import SparkSession, DataFrame

from macroeconomy.utils.paths import get_schemas
from macroeconomy.utils.constants import TABLES


class DeltaReader:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schemas = get_schemas()

    def read(
        self,
        layer: str,
        datasource: str = None,
        dataset: str = None,
        table: str = None,
        run_mode: str = "batch",
    ) -> DataFrame:

        table_name = self._resolve_table(
            layer=layer,
            datasource=datasource,
            dataset=dataset,
            table=table
        )
        if run_mode == "streaming":
            return (
                self.spark.readStream
                .table(table_name)
            )

        return (
            self.spark.read
            .table(table_name)
        )

    def _resolve_table(
        self,
        layer: str,
        datasource: str = None,
        dataset: str = None,
        table: str = None,
    ) -> str:

        # ----------------------------------------------------------
        # Tabla proporcionada directamente
        # ----------------------------------------------------------

        if table is not None:

            if datasource is not None or dataset is not None:
                raise ValueError(
                    "No se puede proporcionar 'table' junto con "
                    "'datasource' o 'dataset'."
                )

            return table

        # ----------------------------------------------------------
        # Datasource + dataset
        # ----------------------------------------------------------

        if datasource is None or dataset is None:
            raise ValueError(
                "Debe proporcionar 'table' o ambos "
                "'datasource' y 'dataset'."
            )

        schema = self.schemas[layer]

        return (
            f"{schema}.{TABLES[datasource][dataset]}"
        )