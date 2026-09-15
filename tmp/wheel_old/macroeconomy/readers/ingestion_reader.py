"""Reader de datos ingestados en landing"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from macroeconomy.utils.paths import get_bronze_root, get_landing_root

from macroeconomy.utils.config import load_confluent_config
from macroeconomy.utils.constants import (
    DEFAULT_CONFLUENT_CONFIG_PATH,
    SOURCE_FORMAT_CLOUDFILES,
    SOURCE_FORMAT_KAFKA,
    VALUE_FORMAT_STRING,
    VALUE_FORMAT_JSON,
    VALUE_FORMAT_AVRO,
)


class IngestionReader:
    """Lee ficheros de landing o streams de Kafka en DataFrames de Spark."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(
        self,
        datasource: str,
        dataset: str,
        source_config: dict,
    ) -> DataFrame:
        fmt = source_config["format"]

        if fmt == SOURCE_FORMAT_CLOUDFILES:
            return self._read_cloudfiles(datasource, dataset, source_config)

        if fmt == SOURCE_FORMAT_KAFKA:
            return self._read_kafka(dataset, source_config)

        raise NotImplementedError(f"Unsupported format '{fmt}'")

    def _read_cloudfiles(
        self,
        datasource: str,
        dataset: str,
        source_config: dict,
    ) -> DataFrame:
        schema_location = f"{get_bronze_root()}/schemas/{datasource}/{dataset}"
        path = f"{get_landing_root()}/{datasource}/{dataset}"
        df = (
            self.spark.readStream.format(SOURCE_FORMAT_CLOUDFILES)
            .options(**source_config["options"])
            .option("cloudFiles.schemaLocation", schema_location)
            .load(path)
        )
        return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
            "_source_file", F.input_file_name()
        )

    def _read_kafka(
        self,
        dataset: str,
        ingestion_config: dict,
    ) -> DataFrame:
        kafka_config = load_confluent_config(DEFAULT_CONFLUENT_CONFIG_PATH)
        topic = ingestion_config["options"].get("subscribe")
        kafka_options = {
            "kafka.bootstrap.servers": kafka_config["bootstrap.servers"],
            "kafka.security.protocol": kafka_config["security.protocol"],
            "kafka.sasl.mechanism": kafka_config["sasl.mechanisms"],
            "kafka.sasl.jaas.config": f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config.get('sasl.username')}" password="{kafka_config.get('sasl.password')}"; """,
            "subscribe": topic,
            "includeHeaders": "true",
            "startingOffsets": "earliest",
        }

        df = (
            self.spark.readStream.format(SOURCE_FORMAT_KAFKA)
            .options(**kafka_options)
            .load()
        )

        columns = [F.col(column).alias(f"_{column}") for column in df.columns]
        df = df.select(*columns)

        value_format = ingestion_config.get("value_format", VALUE_FORMAT_STRING)

        if value_format == VALUE_FORMAT_JSON:
            df = df.withColumn(
                "value",
                F.from_json(
                    F.col("_value").cast("string"),
                    ingestion_config.get("json_schema"),
                ),
            )

        if value_format == VALUE_FORMAT_AVRO:
            raise NotImplementedError("Avro ingestion is not implemented yet.")

        return (
            df.withColumn("_ingested_at", F.current_timestamp())
            .select("*", "value.*")
            .drop("value")
        )
