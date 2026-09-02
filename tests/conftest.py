import os
import sys
import tempfile
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    warehouse_dir = os.path.join(tempfile.gettempdir(), f"pytest_spark_warehouse_{os.getuid()}")
    os.makedirs(warehouse_dir, exist_ok=True)

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-pyspark-local")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )
    yield spark
    spark.stop()
