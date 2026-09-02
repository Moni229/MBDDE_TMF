"""Fuente HTTP de Eurostat para conjuntos de datos estadísticos."""

import requests

from macroeconomy.sources.datasource import DataSource
from macroeconomy.utils.constants import EUROSTAT_CONFIG_KEY


class EurostatSource(DataSource):
    """Obtiene conjuntos de datos de Eurostat por HTTP."""

    def __init__(
        self,
        base_url="https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data",
        time: list[str] | None = None,
    ):
        self.base_url = base_url
        self.time = time

    @property
    def config_key(self):
        return EUROSTAT_CONFIG_KEY

    def read(self, dataset, **params):
        url = f"{self.base_url}/{dataset}"
        request_params = params.copy() if params else {}

        if self.time is not None:
            request_params["time"] = self.time

        response = requests.get(
            url,
            params=request_params,
            timeout=60,
        )

        response.raise_for_status()
        return response.json()
