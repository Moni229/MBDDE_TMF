import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from macroeconomy.writers.landing_writer import LandingWriter


def test_write_dict_writes_text_file(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "macroeconomy.writers.landing_writer.get_landing_paths",
        lambda: {"yahoo": str(tmp_path)},
    )

    lw = LandingWriter(spark)

    ts = datetime(2020, 5, 6, 12, 0, tzinfo=timezone.utc)

    out_path = lw.write({"a": 1, "b": "x"}, "yahoo", "NVDA", timestamp=ts)

    expected = Path(tmp_path) / "NVDA" / "year=2020" / "month=05" / "day=06"
    assert Path(out_path) == expected

    files = list(expected.iterdir())
    assert any(f.is_file() for f in files)

    txt_file = next(f for f in files if f.is_file())
    content = txt_file.read_text(encoding="utf-8").strip()
    loaded = json.loads(content)
    assert loaded == {"a": 1, "b": "x"}


def test_write_pandas_writes_parquet(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "macroeconomy.writers.landing_writer.get_landing_paths",
        lambda: {"yahoo": str(tmp_path)},
    )

    lw = LandingWriter(spark)

    ts = datetime(2021, 8, 9, 0, 0, tzinfo=timezone.utc)

    pdf = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    out_path = lw.write(pdf, "yahoo", "NVDA", timestamp=ts)

    expected = Path(tmp_path) / "NVDA" / "year=2021" / "month=08" / "day=09"
    assert Path(out_path) == expected

    # Parquet directory should contain part files with .parquet
    files = list(expected.rglob("*.parquet"))
    assert len(files) >= 1


def test_write_unsupported_type_raises(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "macroeconomy.writers.landing_writer.get_landing_paths",
        lambda: {"yahoo": str(tmp_path)},
    )

    lw = LandingWriter(spark)

    with pytest.raises(TypeError):
        lw.write(12345, "yahoo", "NVDA")
