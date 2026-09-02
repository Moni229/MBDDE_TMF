import pytest
from unittest.mock import Mock

from macroeconomy.pipelines import bronze_pipeline
from macroeconomy.utils.constants import TABLES


class DummyDF:
    def __init__(self, columns):
        self.columns = columns


def test_run_writes_to_delta_and_returns_query(monkeypatch):
    monkeypatch.setattr(
        "macroeconomy.pipelines.bronze_pipeline.get_schemas",
        lambda: {"bronze": "test_catalog.macroeconomy_bronze"},
    )
    monkeypatch.setattr(
        "macroeconomy.pipelines.bronze_pipeline.get_layer_root",
        lambda layer: "/tmp/bronze_root",
    )

    bp = bronze_pipeline.BronzePipeline.__new__(bronze_pipeline.BronzePipeline)
    bp.layer_root = "bronze"

    dummy_df = DummyDF(["col1", "col2"])
    reader_mock = Mock()
    reader_mock.read.return_value = dummy_df
    bp.reader = reader_mock

    writer_mock = Mock()
    writer_mock.write.return_value = "query_object"
    bp.writer = writer_mock

    bp.pipeline_configs = {
        "yahoo": {
            "source": {"format": "csv"},
            "sinks": {"bronze": {"mode": "append"}},
            "datasets": ["NVDA"],
        }
    }

    result = bp.run("yahoo")

    assert result == "query_object"
    reader_mock.read.assert_called_once_with("yahoo", "NVDA", {"format": "csv"})

    table_name = TABLES["yahoo"]["NVDA"]
    expected_target_table = f"test_catalog.macroeconomy_bronze.{table_name}"
    expected_target_path = "/tmp/bronze_root/yahoo/NVDA"
    expected_query_name = f"bronze-yahoo-NVDA"

    writer_mock.write.assert_called_once_with(
        dummy_df, {"mode": "append"}, expected_target_table, expected_target_path, expected_query_name
    )


def test_run_raises_when_no_dataset_and_no_config_dataset():
    bp = bronze_pipeline.BronzePipeline.__new__(bronze_pipeline.BronzePipeline)
    bp.layer_root = "bronze"
    bp.reader = Mock()
    bp.writer = Mock()

    bp.pipeline_configs = {
        "some_source": {"source": {}, "sinks": {"bronze": {}}}
    }

    with pytest.raises(ValueError):
        bp.run("some_source")
