import pytest
from unittest.mock import Mock

from macroeconomy.writers.delta_writer import DeltaWriter
from macroeconomy.utils.constants import DELTA_FORMAT, DEFAULT_STREAMING_TRIGGER_TIME, WRITE_MODE_OVERWRITE


def test_write_batch_saves_table_with_options_and_returns_none():
    df = Mock()
    df.write = Mock()
    writer_chain = Mock()
    df.write.format.return_value = writer_chain
    writer_chain.options.return_value = writer_chain
    writer_chain.mode.return_value = writer_chain
    writer_chain.saveAsTable = Mock()

    dw = DeltaWriter(layer="bronze")
    target_table = "schema.table"
    target_path = "/tmp/path"

    result = dw.write(
        df,
        sink_config={},
        target_table=target_table,
        target_path=target_path,
        query_name="q",
    )

    df.write.format.assert_called_once_with(DELTA_FORMAT)
    writer_chain.options.assert_called_once()
    called_kwargs = writer_chain.options.call_args[1]
    assert called_kwargs["path"] == target_path
    assert called_kwargs["mergeSchema"] == "true"

    writer_chain.saveAsTable.assert_called_once_with(target_table)
    assert result is None


def test_write_batch_overwrite_with_partitions_uses_replace_where(monkeypatch):
    df = Mock()
    df.write = Mock()
    writer_chain = Mock()
    df.write.format.return_value = writer_chain
    writer_chain.options.return_value = writer_chain
    writer_chain.mode.return_value = writer_chain
    writer_chain.partitionBy.return_value = writer_chain
    writer_chain.option.return_value = writer_chain
    writer_chain.saveAsTable = Mock()

    dw = DeltaWriter(layer="bronze")
    monkeypatch.setattr(dw, "_infer_replace_where", lambda df, partition_cols: "y = 1")

    sink = {"mode": WRITE_MODE_OVERWRITE, "partitionBy": ["y"]}

    dw.write(df, sink, "t", "/p", "q")

    writer_chain.option.assert_called()
    assert any(call[0][0] == "replaceWhere" for call in writer_chain.option.call_args_list)


def test_write_streaming_returns_query_object_and_sets_checkpoint():
    df = Mock()
    df.writeStream = Mock()
    writer_chain = Mock()
    df.writeStream.format.return_value = writer_chain
    writer_chain.options.return_value = writer_chain
    writer_chain.option.return_value = writer_chain
    writer_chain.queryName.return_value = writer_chain
    writer_chain.outputMode.return_value = writer_chain
    writer_chain.trigger.return_value = writer_chain
    writer_chain.toTable.return_value = "stream_query"

    dw = DeltaWriter(layer="bronze")
    res = dw.write(
        df,
        sink_config={"run_mode": "streaming", "mode": "append"},
        target_table="t",
        target_path="/tmp/target",
        query_name="qname",
    )

    assert res == "stream_query"
    df.writeStream.format.assert_called_once_with(DELTA_FORMAT)
    writer_chain.option.assert_any_call("checkpointLocation", "/tmp/target/_checkpoint")
    writer_chain.trigger.assert_called_once_with(processingTime=DEFAULT_STREAMING_TRIGGER_TIME)


def test_write_available_now_triggers_availableNow_and_returns_query():
    df = Mock()
    df.writeStream = Mock()
    writer_chain = Mock()
    df.writeStream.format.return_value = writer_chain
    writer_chain.options.return_value = writer_chain
    writer_chain.option.return_value = writer_chain
    writer_chain.queryName.return_value = writer_chain
    writer_chain.outputMode.return_value = writer_chain
    writer_chain.partitionBy.return_value = writer_chain
    writer_chain.trigger.return_value = writer_chain
    writer_chain.toTable.return_value = "available_query"

    dw = DeltaWriter(layer="bronze")
    res = dw.write(
        df,
        sink_config={"run_mode": "available_now", "output_mode": "append", "partitionBy": ["p"]},
        target_table="t2",
        target_path="/tmp/target2",
        query_name="q2",
    )

    assert res == "available_query"
    writer_chain.trigger.assert_called_once_with(availableNow=True)
    writer_chain.option.assert_any_call("checkpointLocation", "/tmp/target2/_checkpoint")


def test_write_invalid_run_mode_raises():
    df = Mock()
    dw = DeltaWriter(layer="bronze")

    with pytest.raises(ValueError):
        dw.write(df, sink_config={"run_mode": "invalid"}, target_table="t", target_path="p", query_name="q")


def test_infer_replace_where_raises_on_missing_columns():
    df = Mock()
    df.columns = ["a"]
    dw = DeltaWriter(layer="bronze")
    with pytest.raises(ValueError):
        dw._infer_replace_where(df, ["b"])


def test_infer_replace_where_raises_on_empty_partitions():
    df = Mock()
    df.columns = ["y", "m"]

    class Sel:
        def distinct(self):
            return self

        def collect(self):
            return []

    def select_stub(*cols):
        return Sel()

    df.select = Mock(side_effect=select_stub)

    dw = DeltaWriter(layer="bronze")
    with pytest.raises(ValueError):
        dw._infer_replace_where(df, ["y", "m"])


def test_infer_replace_where_builds_correct_conditions():
    df = Mock()
    df.columns = ["y", "m"]

    partitions = [
        {"y": 2020, "m": 1},
        {"y": 2021, "m": None},
        {"y": "O'R", "m": 3},
    ]

    class Sel:
        def __init__(self, parts):
            self._parts = parts

        def distinct(self):
            return self

        def collect(self):
            return self._parts

    def select_stub(*cols):
        return Sel(partitions)

    df.select = Mock(side_effect=select_stub)

    dw = DeltaWriter(layer="bronze")
    result = dw._infer_replace_where(df, ["y", "m"])

    expected = "(y = 2020 AND m = 1) OR (y = 2021 AND m IS NULL) OR (y = 'O''R' AND m = 3)"
    assert result == expected
