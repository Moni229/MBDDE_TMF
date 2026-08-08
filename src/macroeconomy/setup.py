from pyspark.sql import SparkSession

from macroeconomy.utils.paths import get_schemas


def setup_lakehouse(spark: SparkSession | None = None) -> None:
    """
    Creates the FarmIA schemas and operational Delta tables if they don't exist.

    Can be called from a notebook, a job entry-point, or a CI pipeline.

    Args:
        spark: Active SparkSession. If None, the running session is used.
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession found.")

    schemas = get_schemas()

    for schema in schemas.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    print("Lakehouse Macroeconomy ✅")
