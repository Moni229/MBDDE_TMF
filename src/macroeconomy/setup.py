from pyspark.sql import SparkSession

from macroeconomy.utils.paths import get_schemas


def setup_lakehouse(spark: SparkSession | None = None) -> None:
    """
    Crea los esquemas de FarmIA y las tablas Delta operacionales si no existen.

    Puede llamarse desde un notebook, un punto de entrada de job o un pipeline de CI.

    Parámetros:
        spark: SparkSession activa. Si es None, se usa la sesión en ejecución.
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No se encontró ninguna SparkSession activa.")

    schemas = get_schemas()

    for schema in schemas.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    print("Lakehouse Macroeconomy ✅")
