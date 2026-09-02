import pytest
from unittest.mock import Mock, ANY

import macroeconomy.readers.ingestion_reader as ir_mod
from macroeconomy.readers.ingestion_reader import IngestionReader
from macroeconomy.utils.constants import (
    SOURCE_FORMAT_CLOUDFILES,
    SOURCE_FORMAT_KAFKA,
    VALUE_FORMAT_JSON,
    VALUE_FORMAT_AVRO,
)


class SimpleBuilder:
    """Minimal builder that records calls and returns a provided df."""

    def __init__(self, df):
        self.df = df
        self.fmt = None
        self.opts = {}
        self.opt_kv = {}
        self.load_path = None

    def format(self, fmt):
        self.fmt = fmt
        return self

    def options(self, **kwargs):
        self.opts.update(kwargs)
        return self

    def option(self, key, value):
        self.opt_kv[key] = value
        return self

    def load(self, path=None):
        self.load_path = path
        return self.df


def test_read_cloudfiles_calls_builder_and_adds_columns(spark, monkeypatch):
    df = spark.createDataFrame([{"x": 1}])

    builder = SimpleBuilder(df)

    fake_spark = Mock()
    fake_spark.readStream = Mock()
    fake_spark.readStream.format = builder.format

    monkeypatch.setattr(ir_mod, "get_bronze_root", lambda: "/bronze_root")
    monkeypatch.setattr(ir_mod, "get_landing_root", lambda: "/landing_root")

    ir = IngestionReader.__new__(IngestionReader)
    ir.spark = fake_spark

    out = ir._read_cloudfiles("yahoo", "NVDA", {"options": {"cloud": "yes"}})

    assert builder.fmt == SOURCE_FORMAT_CLOUDFILES
    assert builder.opt_kv["cloudFiles.schemaLocation"] == "/bronze_root/schemas/yahoo/NVDA"
    assert builder.load_path == "/landing_root/yahoo/NVDA"
    assert "_ingested_at" in out.columns
    assert "_source_file" in out.columns
    assert out is not None


def test_read_kafka_json_parses_and_adds_ingested_and_drops_value(spark, monkeypatch):
    kafka_cfg = {
        "bootstrap.servers": "host:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "user",
    }
    monkeypatch.setattr(ir_mod, "load_confluent_config", lambda path: kafka_cfg)

    data = [("k1", '{"a":"A","b":"B"}'), ("k2", '{"a":"C","b":"D"}')]
    df = spark.createDataFrame(data, schema=["key", "value"])

    builder = SimpleBuilder(df)

    fake_spark = Mock()
    fake_spark.readStream = Mock()
    fake_spark.readStream.format = builder.format

    from pyspark.sql.types import StructType, StructField, StringType

    json_schema = StructType([StructField("a", StringType()), StructField("b", StringType())])

    ir = IngestionReader.__new__(IngestionReader)
    ir.spark = fake_spark

    ingestion_config = {"options": {"subscribe": "topic1"}, "value_format": VALUE_FORMAT_JSON, "json_schema": json_schema}

    out = ir._read_kafka("NVDA", ingestion_config)

    assert builder.fmt == SOURCE_FORMAT_KAFKA
    assert builder.opts.get("subscribe") == "topic1"
    assert builder.opts.get("kafka.bootstrap.servers") == "host:9092"

    cols = out.columns
    assert "_ingested_at" in cols
    assert "a" in cols and "b" in cols
    assert "value" not in cols


def test_read_kafka_avro_raises(spark, monkeypatch):
    kafka_cfg = {"bootstrap.servers": "h:1", "security.protocol": "p", "sasl.mechanisms": "m"}
    monkeypatch.setattr(ir_mod, "load_confluent_config", lambda path: kafka_cfg)

    from pyspark.sql.types import StructType, StructField, StringType
    empty_schema = StructType([StructField("key", StringType()), StructField("value", StringType())])
    df = spark.createDataFrame([], schema=empty_schema)
    builder = SimpleBuilder(df)

    fake_spark = Mock()
    fake_spark.readStream = Mock()
    fake_spark.readStream.format = builder.format

    ir = IngestionReader.__new__(IngestionReader)
    ir.spark = fake_spark

    ingestion_config = {"options": {"subscribe": "t"}, "value_format": VALUE_FORMAT_AVRO}

    with pytest.raises(NotImplementedError):
        ir._read_kafka("NVDA", ingestion_config)


def test_read_unsupported_format_raises():
    ir = IngestionReader.__new__(IngestionReader)
    with pytest.raises(NotImplementedError):
        ir.read("yahoo", "NVDA", {"format": "unknown"})
