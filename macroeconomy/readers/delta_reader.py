"""Readers para Delta tables en batch y streaming"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from macroeconomy.utils.constants import (
    RUN_MODE_BATCH,
    RUN_MODE_STREAMING,
    TABLES,
)
from macroeconomy.utils.paths import get_schemas


class DeltaReader:
    """Lee tablas Delta y, opcionalmente, aplica filtros de partición"""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schemas = get_schemas()

    def read(
        self,
        layer: str,
        datasource: str = None,
        dataset: str = None,
        table: str = None,
        run_mode: str = RUN_MODE_BATCH,
        partitions: dict = None,
    ) -> DataFrame:

        table_name = self._resolve_table(
            layer=layer,
            datasource=datasource,
            dataset=dataset,
            table=table,
        )

        if run_mode == RUN_MODE_STREAMING:
            return self.spark.readStream.table(table_name)

        if run_mode != RUN_MODE_BATCH:
            raise ValueError(
                f"Unsupported run_mode '{run_mode}'. "
                f"Expected: '{RUN_MODE_BATCH}' or '{RUN_MODE_STREAMING}'."
            )

        df = self.spark.read.table(table_name)

        if partitions:
            df = self._apply_filters(df, partitions)

        return df

    def _apply_filters(
        self,
        df: DataFrame,
        filters: dict,
    ) -> DataFrame:
        missing_columns = set(filters.keys()) - set(df.columns)

        if missing_columns:
            raise ValueError(
                "Las columnas utilizadas en los filtros "
                f"no existen en el DataFrame: "
                f"{sorted(missing_columns)}"
            )

        for column, value in filters.items():
            if value is None:
                df = df.filter(col(column).isNull())
            elif isinstance(value, (list, tuple, set)):
                df = df.filter(col(column).isin(list(value)))
            else:
                df = df.filter(col(column) == value)

        return df

    def _resolve_table(
        self,
        layer: str,
        datasource: str = None,
        dataset: str = None,
        table: str = None,
    ) -> str:
        if table is not None:
            if datasource is not None or dataset is not None:
                raise ValueError(
                    "No se puede proporcionar 'table' junto con "
                    "'datasource' o 'dataset'."
                )

            return table

        if datasource is None or dataset is None:
            raise ValueError(
                "Debe proporcionar 'table' o ambos " "'datasource' y 'dataset'."
            )

        schema = self.schemas[layer]

        return f"{schema}.{TABLES[datasource][dataset]}"
