from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

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
        partitions: dict = None,
    ) -> DataFrame:

        table_name = self._resolve_table(
            layer=layer,
            datasource=datasource,
            dataset=dataset,
            table=table,
        )

        if run_mode == "streaming":

            return (
                self.spark.readStream
                .table(table_name)
            )

        if run_mode != "batch":
            raise ValueError(
                f"Unsupported run_mode '{run_mode}'. "
                "Expected: 'batch' or 'streaming'."
            )

        df = (
            self.spark.read
            .table(table_name)
        )

        if partitions:
            df = self._apply_filters(
                df,
                partitions,
            )

        return df

    def _apply_filters(
        self,
        df: DataFrame,
        filters: dict,
    ) -> DataFrame:

        # ----------------------------------------------------------
        # Comprobar columnas
        # ----------------------------------------------------------

        missing_columns = (
            set(filters.keys()) - set(df.columns)
        )

        if missing_columns:
            raise ValueError(
                "Las columnas utilizadas en los filtros "
                f"no existen en el DataFrame: "
                f"{sorted(missing_columns)}"
            )

        # ----------------------------------------------------------
        # Aplicar filtros
        #
        # Ejemplo:
        #
        # filters = {
        #     "year": 2028,
        #     "month": 8,
        #     "day": 12,
        # }
        #
        # genera:
        #
        # year = 2028
        # AND month = 8
        # AND day = 12
        # ----------------------------------------------------------

        for column, value in filters.items():

            if value is None:
                df = df.filter(
                    col(column).isNull()
                )

            elif isinstance(value, (list, tuple, set)):
                df = df.filter(
                    col(column).isin(list(value))
                )

            else:
                df = df.filter(
                    col(column) == value
                )

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
                "Debe proporcionar 'table' o ambos "
                "'datasource' y 'dataset'."
            )

        schema = self.schemas[layer]

        return (
            f"{schema}.{TABLES[datasource][dataset]}"
        )