from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
#from confluent_kafka.schema_registry import SchemaRegistryClient

from macroeconomy.utils.paths import get_bronze_root, get_landing_root

from macroeconomy.utils.config import load_confluent_config

from macroeconomy.utils.paths import get_landing_root


class IngestionReader:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(
            self,
            datasource: str,
            dataset: str,
            source_config: dict, #igual lo cambio por el nombre de la source y dentro pillo la confi especifica
            # me faltaria el schema_registry y el client_props_path
    ) -> DataFrame:
        fmt = source_config["format"]

        if fmt == "cloudFiles":
            return self._read_cloudfiles(datasource, dataset, source_config)

        elif fmt == "kafka":
            return self._read_kafka(dataset, source_config)

        else:
            raise NotImplementedError(f"Unsupported format '{fmt}'")

    # ------------------------------------------------------------------

    def _read_cloudfiles(
            self,
            datasource: str,
            dataset: str,
            source_config: dict,
    ) -> DataFrame:
        schema_location = f"{get_bronze_root()}/{datasource}/{dataset}"
        path = f"{get_landing_root()}/{datasource}/{dataset}"
        df = (
            self.spark.readStream.format("cloudFiles")
            .options(**source_config["options"])
            .option("cloudFiles.schemaLocation", schema_location)
            .load(path)
        )
        return (
            df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.input_file_name())
        )

    # ------------------------------------------------------------------

    def _read_kafka(
            self,
            dataset: str,
            ingestion_config: dict,
            # schema_registry_client: SchemaRegistryClient = None,
    ) -> DataFrame:
        kafka_config = load_confluent_config()
        topic = ingestion_config['options'].get('subscribe')
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
            self.spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
        )

        columns = [F.col(column).alias(f"_{column}") for column in df.columns]
        df = df.select(*columns)

        value_format = ingestion_config.get("value_format", "string")

        if value_format == "json":
            df = df.withColumn(
                "value",
                F.from_json(
                    F.col("_value").cast("string"),
                    ingestion_config.get("json_schema"),
                ),
            )

        if value_format == "avro":
            if not schema_registry_client:
                raise ValueError("schema_registry_conf is required for avro format.")
            value_subject = f"{topic}-value"
            value_schema = schema_registry_client.get_latest_version(
                value_subject
            ).schema.schema_str
            df = df.withColumn("key", F.col("_key").cast("string")).withColumn(
                "value",
                # from_avro(F.expr("substring(_value,6,length(_value)-5)"), value_schema),
            )

        return (
            df.withColumn("_ingested_at", F.current_timestamp())
            .select("*", "value.*")
            .drop("value")
        )
