import pytest
from unittest.mock import Mock

from macroeconomy.pipelines import gold_pipeline
from macroeconomy.pipelines.gold_pipeline import GoldPipeline
from macroeconomy.utils.constants import GOLD, RUN_MODE_BATCH, RUN_MODE_STREAMING


def make_reader_return(name, count_value=1):
    df = Mock()
    df.count.return_value = count_value
    return df


def test_run_calls_reader_transform_and_writer(monkeypatch):
    gp = GoldPipeline.__new__(GoldPipeline)
    gp.layer = GOLD

    reader = Mock()
    reader.read.side_effect = [make_reader_return('t1', 3), make_reader_return('t2', 4)]
    gp.reader = reader

    writer = Mock()
    gp.writer = writer

    etl = Mock()
    gold_df = Mock()
    etl.transform.return_value = gold_df
    monkeypatch.setattr(gold_pipeline, "get_etl", lambda layer, name: etl)

    gp.gold_configs = {
        "etl1": {
            "source": ["t1", "t2"],
            "options": {"opt": True},
            "sink": {"target_table": "gold_tbl"},
        }
    }

    monkeypatch.setattr(gold_pipeline, "get_schemas", lambda: {GOLD: "test_schema"})
    monkeypatch.setattr(gold_pipeline, "get_layer_root", lambda layer: "/tmp/layer_root")

    gp.run("etl1", partitions=[{"p": 1}])

    assert reader.read.call_count == 2
    reader.read.assert_any_call(layer=GOLD, table="t1", run_mode=RUN_MODE_BATCH, partitions=[{"p": 1}])
    reader.read.assert_any_call(layer=GOLD, table="t2", run_mode=RUN_MODE_BATCH, partitions=[{"p": 1}])

    etl.transform.assert_called_once()
    args, kwargs = etl.transform.call_args
    assert isinstance(args[0], dict)
    assert args[1] == {"opt": True}

    target_table = "test_schema.gold_tbl"
    target_path = "/tmp/layer_root/etl1"
    query_name = f"{GOLD}-etl1"
    writer.write.assert_called_once_with(gold_df, gp.gold_configs["etl1"]["sink"], target_table, target_path, query_name)


def test_run_respects_sink_run_mode(monkeypatch):
    gp = GoldPipeline.__new__(GoldPipeline)
    gp.layer = GOLD

    reader = Mock()
    reader.read.return_value = make_reader_return('only', 2)
    gp.reader = reader

    writer = Mock()
    gp.writer = writer

    etl = Mock()
    etl.transform.return_value = Mock()
    monkeypatch.setattr(gold_pipeline, "get_etl", lambda layer, name: etl)

    gp.gold_configs = {
        "stream_etl": {
            "source": ["only"],
            "sink": {"target_table": "g", "run_mode": RUN_MODE_STREAMING},
        }
    }

    monkeypatch.setattr(gold_pipeline, "get_schemas", lambda: {GOLD: "s"})
    monkeypatch.setattr(gold_pipeline, "get_layer_root", lambda layer: "/root")

    gp.run("stream_etl")

    reader.read.assert_called_once_with(layer=GOLD, table="only", run_mode=RUN_MODE_STREAMING, partitions=None)
    writer.write.assert_called_once()
