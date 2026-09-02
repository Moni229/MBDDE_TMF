import pytest
from unittest.mock import Mock

from macroeconomy.pipelines.silver_pipeline import SilverPipeline
from macroeconomy.utils.constants import BRONZE, SILVER, TABLES


def test_run_reads_transforms_and_writes(monkeypatch):
    sp = SilverPipeline.__new__(SilverPipeline)

    reader = Mock()
    reader.read.return_value = "bronze_df"
    sp.reader = reader

    etl = Mock()
    etl.transform.return_value = "silver_df"
    monkeypatch.setattr("macroeconomy.pipelines.silver_pipeline.get_etl", lambda layer, name: etl)

    writer = Mock()
    writer.write.return_value = "query_obj"
    sp.writer = writer

    sp.pipeline_configs = {
        "yahoo": {"sinks": {"silver": {}}}
    }

    sp.layer = SILVER

    monkeypatch.setattr("macroeconomy.pipelines.silver_pipeline.get_schemas", lambda: {SILVER: "sch"})
    monkeypatch.setattr("macroeconomy.pipelines.silver_pipeline.get_layer_root", lambda layer: "/root")

    res = sp.run("yahoo", "NVDA")

    assert res == "query_obj"
    reader.read.assert_called_once_with(layer=BRONZE, datasource="yahoo", dataset="NVDA", run_mode="batch", partitions=None)
    etl.transform.assert_called_once_with("bronze_df")
    writer.write.assert_called_once()


def test_run_uses_sink_run_mode(monkeypatch):
    sp = SilverPipeline.__new__(SilverPipeline)

    reader = Mock()
    reader.read.return_value = "b"
    sp.reader = reader

    etl = Mock()
    etl.transform.return_value = "s"
    monkeypatch.setattr("macroeconomy.pipelines.silver_pipeline.get_etl", lambda layer, name: etl)

    writer = Mock()
    sp.writer = writer

    sp.pipeline_configs = {
        "yahoo": {"sinks": {"silver": {"run_mode": "streaming"}}}
    }

    sp.layer = SILVER
    monkeypatch.setattr("macroeconomy.pipelines.silver_pipeline.get_schemas", lambda: {SILVER: "sch"})
    monkeypatch.setattr("macroeconomy.pipelines.silver_pipeline.get_layer_root", lambda layer: "/root")

    sp.run("yahoo", "NVDA")

    reader.read.assert_called_once_with(layer=BRONZE, datasource="yahoo", dataset="NVDA", run_mode="streaming", partitions=None)


def test_run_raises_when_no_pipeline_config_for_datasource():
    sp = SilverPipeline.__new__(SilverPipeline)
    sp.pipeline_configs = {}
    sp.reader = Mock()
    sp.writer = Mock()
    sp.layer = SILVER

    with pytest.raises(KeyError):
        sp.run("missing", "NVDA")


def test_run_raises_when_dataset_not_in_tables(monkeypatch):
    sp = SilverPipeline.__new__(SilverPipeline)
    sp.pipeline_configs = {"yahoo": {"sinks": {"silver": {}}}}
    sp.reader = Mock()
    sp.writer = Mock()
    sp.layer = SILVER

    monkeypatch.setattr("macroeconomy.pipelines.silver_pipeline.TABLES", {"yahoo": {}}, raising=False)

    with pytest.raises(KeyError):
        sp.run("yahoo", "UNKNOWN")


def test_run_bubbles_etl_exception(monkeypatch):
    sp = SilverPipeline.__new__(SilverPipeline)
    sp.pipeline_configs = {"yahoo": {"sinks": {"silver": {}}}}
    sp.reader = Mock()

    class BadETL:
        def transform(self, df):
            raise RuntimeError("boom")

    monkeypatch.setattr("macroeconomy.pipelines.silver_pipeline.get_etl", lambda layer, name: BadETL())
    sp.writer = Mock()
    sp.layer = SILVER

    with pytest.raises(RuntimeError):
        sp.run("yahoo", "NVDA")
