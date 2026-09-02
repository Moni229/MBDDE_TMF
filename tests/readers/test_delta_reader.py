import pytest
from unittest.mock import Mock

from macroeconomy.readers.delta_reader import DeltaReader
from macroeconomy.utils.constants import RUN_MODE_BATCH, RUN_MODE_STREAMING, TABLES



def test_resolve_table_with_explicit_table_and_other_args_raises():
    dr = DeltaReader.__new__(DeltaReader)
    with pytest.raises(ValueError):
        dr._resolve_table(layer="bronze", table="some.table", datasource="yahoo")


def test_resolve_table_returns_table_when_table_provided():
    dr = DeltaReader.__new__(DeltaReader)
    assert dr._resolve_table(layer="any", table="custom.table") == "custom.table"


def test_resolve_table_builds_from_schema_and_tables():
    dr = DeltaReader.__new__(DeltaReader)
    dr.schemas = {"bronze": "test_schema"}
    table_name = TABLES["yahoo"]["NVDA"]
    result = dr._resolve_table(layer="bronze", datasource="yahoo", dataset="NVDA")
    assert result == f"test_schema.{table_name}"


def test_apply_filters_raises_on_missing_columns():
    dr = DeltaReader.__new__(DeltaReader)
    # minimal mock dataframe lacking column 'c'
    mock_df = Mock()
    mock_df.columns = ["a", "b"]

    with pytest.raises(ValueError) as exc:
        dr._apply_filters(mock_df, {"c": 1})
    assert "c" in str(exc.value)


def test_apply_filters_applies_null_list_and_scalar_filters(spark):
    dr = DeltaReader.__new__(DeltaReader)

    data = [
        (None, "x", "z"),
        ("v", "y", "z"),
        (None, "y", "z"),
        ("u", "x", "n")
    ]
    df = spark.createDataFrame(data, schema=["col1", "col2", "col3"])

    out = dr._apply_filters(df, {"col1": None, "col2": ["x", "y"], "col3": "z"})

    rows = out.collect()
    print('DEBUG rows:', rows)
    assert len(rows) == 2
    for r in rows:
        assert r[0] is None
        assert r[1] in ("x", "y")
        assert r[2] == "z"


def test_read_streams_and_batches_and_partitions_and_invalid_mode():
    dr = DeltaReader.__new__(DeltaReader)

    dr._resolve_table = Mock(return_value="catalog.schema.table")

    spark = Mock()
    spark.read = Mock()
    spark.read.table = Mock(return_value="batch_df")
    spark.readStream = Mock()
    spark.readStream.table = Mock(return_value="stream_df")
    dr.spark = spark

    res_stream = dr.read(layer="any", run_mode=RUN_MODE_STREAMING)
    assert res_stream == "stream_df"
    spark.readStream.table.assert_called_once_with("catalog.schema.table")

    dr._apply_filters = Mock(return_value="filtered_df")
    res_batch = dr.read(layer="any", run_mode=RUN_MODE_BATCH, partitions={"p": 1})
    assert res_batch == "filtered_df"
    spark.read.table.assert_called_once_with("catalog.schema.table")
    dr._apply_filters.assert_called_once_with("batch_df", {"p": 1})

    with pytest.raises(ValueError):
        dr.read(layer="any", run_mode="not-a-mode")
