import pytest

import macroeconomy.utils.transformers as transformers
from macroeconomy.etls.etl_class import ETLClass


def test_get_etl_returns_instance_for_registered(monkeypatch):
    class DummyETL(ETLClass):
        def transform(self, df):
            return "transformed"

    monkeypatch.setitem(transformers.SILVER_ETL_REGISTRY, "dummy_ds", DummyETL)

    inst = transformers.get_etl("silver", "dummy_ds")
    assert isinstance(inst, DummyETL)
    assert isinstance(inst, ETLClass)


def test_get_etl_is_case_insensitive_for_layer(monkeypatch):
    class OtherETL(ETLClass):
        def transform(self, df):
            return "ok"

    monkeypatch.setitem(transformers.GOLD_ETL_REGISTRY, "my_gold", OtherETL)

    inst = transformers.get_etl("GOLD", "my_gold")
    assert isinstance(inst, OtherETL)


def test_get_etl_unsupported_layer_raises():
    with pytest.raises(ValueError) as exc:
        transformers.get_etl("bronze", "whatever")
    assert "Unsupported layer" in str(exc.value)


def test_get_etl_unconfigured_datasource_raises():
    # use an existing layer but ask for a datasource that is not present
    with pytest.raises(ValueError) as exc:
        transformers.get_etl("silver", "no_such_ds")
    assert "No SILVER transformer configured" in str(exc.value)
