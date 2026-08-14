from macroeconomy.sources.datasource import DataSource
import requests

class EurostatSource(DataSource):

    def __init__(self, base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"):
        self.base_url = base_url

    @property
    def config_key(self):
        return "eurostat"

    def read(self, dataset, time=None, **params):
        url = f"{self.base_url}/{dataset}"

        request_params = params.copy() if params else {}

        if time:
            request_params["time"] = time

        response = requests.get(
            url,
            params=request_params
        )

        response.raise_for_status()

        return response.json()