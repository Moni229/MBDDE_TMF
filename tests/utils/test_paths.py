import importlib
import os
from types import SimpleNamespace

import pytest


def test_get_landing_root_uses_env_var(monkeypatch):
    monkeypatch.setenv("ADLS_ACCOUNT_NAME", "acct123")
    paths = importlib.reload(importlib.import_module("macroeconomy.utils.paths"))

    root = paths.get_landing_root()
    assert "acct123" in root
    assert paths.LANDING_CONTAINER in root


def test_get_landing_paths_uses_root(monkeypatch):
    paths = importlib.import_module("macroeconomy.utils.paths")
    monkeypatch.setattr(paths, "get_landing_root", lambda: "abfss://root")

    p = paths.get_landing_paths()
    assert set(p.keys()) == {"fred", "yahoo", "eurostat", "finnhub"}
    assert p["yahoo"].startswith("abfss://root")


def test_get_catalog_and_schemas(monkeypatch):
    paths = importlib.import_module("macroeconomy.utils.paths")

    fake_catalog = "mycatalog"
    fake_spark = SimpleNamespace(catalog=SimpleNamespace(currentCatalog=lambda: fake_catalog))
    monkeypatch.setattr(paths, "_get_spark", lambda: fake_spark)

    assert paths.get_catalog() == fake_catalog
    schemas = paths.get_schemas()
    assert schemas["bronze"] == f"{fake_catalog}.macroeconomy_bronze"
    assert schemas["gold"] == f"{fake_catalog}.macroeconomy_gold"


def test_get_layer_root_valid_and_invalid(monkeypatch):
    paths = importlib.import_module("macroeconomy.utils.paths")

    monkeypatch.setattr(paths, "get_bronze_root", lambda: "/bronze_root")
    monkeypatch.setattr(paths, "get_silver_root", lambda: "/silver_root")
    monkeypatch.setattr(paths, "get_gold_root", lambda: "/gold_root")

    assert paths.get_layer_root("bronze") == "/bronze_root"
    assert paths.get_layer_root("silver") == "/silver_root"
    assert paths.get_layer_root("gold") == "/gold_root"

    with pytest.raises(ValueError):
        paths.get_layer_root("unknown")


def test_roots_include_storage_account(monkeypatch):
    monkeypatch.setenv("ADLS_ACCOUNT_NAME", "stor001")
    paths = importlib.reload(importlib.import_module("macroeconomy.utils.paths"))

    b = paths.get_bronze_root()
    s = paths.get_silver_root()
    g = paths.get_gold_root()

    assert "stor001" in b
    assert "stor001" in s
    assert "stor001" in g
