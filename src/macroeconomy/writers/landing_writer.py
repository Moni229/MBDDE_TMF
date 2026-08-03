from datetime import datetime
import json

import pandas as pd
from pyspark.sql import SparkSession

from macroeconomy.utils.paths import get_landing_paths


class LandingWriter:

    def __init__(self):
        self.landing_paths = get_landing_paths()

    def write(
        self,
        data,
        source: str,
        dataset: str,
        timestamp: datetime | None = None
    ):
        timestamp = timestamp or datetime.utcnow()

        base_path = self.landing_paths[source]

        path = (
            f"{base_path}/"
            f"{dataset}/"
            f"year={timestamp:%Y}/"
            f"month={timestamp:%m}/"
            f"day={timestamp:%d}"
        )

        spark = SparkSession.getActiveSession()

        if spark is None:
            spark = (
                SparkSession.builder
                .appName("macroeconomy")
                .getOrCreate()
            )
        if isinstance(data, dict):

            # Guardar un único fichero JSON
            df = spark.createDataFrame([(json.dumps(data),)], ["value"])
            (
                df.coalesce(1)
                .write
                .mode("overwrite")
                .text(path)
            )

        elif isinstance(data, pd.DataFrame):

            spark_df = spark.createDataFrame(data)
            (
                spark_df.write
                .mode("overwrite")
                .parquet(path)
            )

        else:
            raise TypeError(f"Tipo no soportado: {type(data)}")

        return path