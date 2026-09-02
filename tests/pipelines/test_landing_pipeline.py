import sys
from types import ModuleType
import pytest
from unittest.mock import Mock, ANY

if "pyspark.sql.connect.session" not in sys.modules:
    fake_pyspark = ModuleType("pyspark")
    fake_sql = ModuleType("pyspark.sql")
    fake_connect = ModuleType("pyspark.sql.connect")
    fake_session = ModuleType("pyspark.sql.connect.session")
    setattr(fake_session, "SparkSession", object)
    fake_connect.session = fake_session
    setattr(fake_sql, "SparkSession", object)
    fake_sql.connect = fake_connect
    fake_pyspark.sql = fake_sql
    sys.modules["pyspark"] = fake_pyspark
    sys.modules["pyspark.sql"] = fake_sql
    sys.modules["pyspark.sql.connect"] = fake_connect
    sys.modules["pyspark.sql.connect.session"] = fake_session

from macroeconomy.pipelines import landing_pipeline
from macroeconomy.pipelines.landing_pipeline import LandingPipeline


class DummySource:
    def __init__(self, config_key="src", streaming=False, read_return=None):
        self._config_key = config_key
        self._streaming = streaming
        self._read_return = read_return

    @property
    def config_key(self):
        return self._config_key

    @property
    def is_streaming(self):
        return self._streaming

    def read(self, **kwargs):
        return self._read_return


def test_run_batch_invokes_writer_for_each_dataset():
    lp = LandingPipeline.__new__(LandingPipeline)
    writer = Mock()
    lp.writer = writer

    lp.ingestion_configs = {
        "mysource": {
            "source": {"format": "csv"},
            "datasets": [
                {"name": "d1", "params": {"p": 1}},
                {"name": "d2", "params": {"q": 2}},
            ],
        }
    }

    src = DummySource(config_key="mysource", streaming=False, read_return={"x": 1})

    lp.run(source=src, source_name="mysource")

    assert writer.write.call_count == 2
    writer.write.assert_any_call(data={"x": 1}, source="mysource", dataset="d1")
    writer.write.assert_any_call(data={"x": 1}, source="mysource", dataset="d2")


def test_run_streaming_registers_callback_and_writer_called(monkeypatch):
    lp = LandingPipeline.__new__(LandingPipeline)
    writer = Mock()
    lp.writer = writer

    lp.ingestion_configs = {
        "streamsrc": {
            "source": {},
            "datasets": [{"name": "ds1"}],
        }
    }

    captured = {}

    def fake_read(dataset, on_data=None, **kwargs):
        captured["on_data"] = on_data
        return None

    src = DummySource(config_key="streamsrc", streaming=True)
    src.read = Mock(side_effect=fake_read)

    lp.run(source=src, source_name="streamsrc")

    src.read.assert_called_once()
    assert "on_data" in captured

    sample = {"a": 1}
    captured["on_data"](sample)

    writer.write.assert_called_once_with(data=sample, source="streamsrc", dataset="ds1")


def test_run_with_dataset_filter_only_runs_matching_dataset():
    lp = LandingPipeline.__new__(LandingPipeline)
    writer = Mock()
    lp.writer = writer

    lp.ingestion_configs = {
        "src": {
            "datasets": [
                {"name": "keep"},
                {"name": "skip"},
            ]
        }
    }

    src = DummySource(config_key="src", streaming=False, read_return="data")

    lp.run(source=src, source_name="src", dataset="keep")

    writer.write.assert_called_once_with(data="data", source="src", dataset="keep")
