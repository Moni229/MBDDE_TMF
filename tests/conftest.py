import os
import sys
import tempfile
import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    warehouse_dir = os.path.join(tempfile.gettempdir(), f"pytest_spark_warehouse_{os.getuid()}")
    os.makedirs(warehouse_dir, exist_ok=True)

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("pytest-pyspark-local")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
