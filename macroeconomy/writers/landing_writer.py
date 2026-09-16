"""Writer de datos origen en landing, unicamente en modo batch"""

from datetime import datetime
import json

import pandas as pd
from pandas import Timestamp
from pyspark.sql import SparkSession

from macroeconomy.utils.paths import get_landing_paths
from macroeconomy.utils.constants import WRITE_MODE_OVERWRITE


class LandingWriter:
    """Persiste los datos de landing como texto JSON o parquet."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.landing_paths = get_landing_paths()

    def write(self, data, source: str, dataset: str, timestamp: datetime | None = None):
        timestamp = timestamp or Timestamp.now("UTC")

        base_path = self.landing_paths[source]

        path = (
            f"{base_path}/"
            f"{dataset}/"
            f"year={timestamp:%Y}/"
            f"month={timestamp:%m}/"
            f"day={timestamp:%d}"
        )

        if isinstance(data, dict):
            df = self.spark.createDataFrame([(json.dumps(data),)], ["value"])
            (df.coalesce(1).write.mode(WRITE_MODE_OVERWRITE).text(path))

        elif isinstance(data, pd.DataFrame):

            spark_df = self.spark.createDataFrame(data)
            (spark_df.write.mode(WRITE_MODE_OVERWRITE).parquet(path))

        else:
            raise TypeError(f"Tipo no soportado: {type(data)}")

        return path
