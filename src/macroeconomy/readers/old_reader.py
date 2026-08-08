"""
farmia.pipelines.engine.reader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Reader generico. Preparado para cloudFiles y para ingestas Kafka
"""

from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from farmia.config import get_bronze_root, read_confluent_config
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

from farmia.utils.image_utils import extract_label, get_extract_size_udf


def read_batch_data(
    spark: SparkSession,
    ingestion_config: dict,
) -> DataFrame:
    datasource = ingestion_config["datasource"]
    dataset = ingestion_config["dataset"]
    source = ingestion_config["source"]
    schema_location = f"{get_bronze_root()}/{datasource}/{dataset}"
    clf_ftm = source["options"].get("cloudFiles.format")

    df = (
        spark.readStream.format("cloudFiles")
        .options(**source["options"])
        .option("cloudFiles.schemaLocation", schema_location)
        .load(source["path"])
    )

    if clf_ftm == "binaryFile":
        image_opts = source.get("image_options", {})
        raw_pattern = image_opts.get("label_pattern", r"/([^/]+)/[^/]+$")
        label_pattern = (
            next(iter(raw_pattern))
            if isinstance(raw_pattern, set)
            else str(raw_pattern)
        )
        extract_size_udf = get_extract_size_udf()

        df = (
            df.withColumn("_size", extract_size_udf(col("content")))
            .withColumn("_label", extract_label(col("path"), label_pattern))
            .coalesce(1)
        )

    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_ingested_file", F.input_file_name()
    )


def read_streaming_data(
    spark: SparkSession,
    ingestion_config: dict,
    kafka_config: dict,
    schema_registry_client: SchemaRegistryClient = None,
) -> DataFrame:
    topic = ingestion_config["source"]["options"].get("subscribe")

    print(f"Lectura del topic {topic}")
    kafka_options = {
        "kafka.bootstrap.servers": kafka_config["bootstrap.servers"],
        "kafka.security.protocol": kafka_config["security.protocol"],
        "kafka.sasl.mechanism": kafka_config["sasl.mechanisms"],
        "kafka.sasl.jaas.config": f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config.get('sasl.username')}" password="{kafka_config.get('sasl.password')}"; """,
        "subscribe": topic,
        "includeHeaders": "true",
        "startingOffsets": "earliest",
    }

    df = spark.readStream.format("kafka").options(**kafka_options).load()

    columns = [F.col(column).alias(f"_{column}") for column in df.columns]
    df = df.select(*columns)

    source = ingestion_config["source"]
    value_format = source.get("value_format", "string")

    if value_format == "json":
        df = df.withColumn(
            "value",
            F.from_json(
                F.col("_value").cast("string"),
                ingestion_config["source"].get("json_schema"),
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
            from_avro(F.expr("substring(_value,6,length(_value)-5)"), value_schema),
        )

    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .select("*", "value.*")
        .drop("value")
    )


def read(
    spark: SparkSession,
    ingestion_config: dict,
    schema_registry_client: SchemaRegistryClient,
    client_props_path: str | None = None,
) -> DataFrame:
    source = ingestion_config["source"]
    fmt = source["format"]

    if fmt == "kafka":
        kafka_config = read_confluent_config(client_props_path)
        df = read_streaming_data(
            spark, ingestion_config, kafka_config, schema_registry_client
        )
    elif fmt in ["cloudFiles", "binaryFile"]:
        df = read_batch_data(spark, ingestion_config)
    else:
        raise NotImplementedError(
            f'Source format "{fmt}" is not implemented. Use "kafka",  "cloudFiles" or "binaryFile".'
        )

    return df.withColumn("_ingested_at", F.current_timestamp())
