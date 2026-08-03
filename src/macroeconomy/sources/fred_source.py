import requests
from macroeconomy.sources.datasource import DataSource


class FredSource(DataSource):

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key):
        self.api_key = api_key

    @property
    def config_key(self):
        return "fred"

    def read(self, dataset):

        response = requests.get(
            self.BASE_URL,
            params={
                "series_id": dataset,
                "api_key": self.api_key,
                "file_type": "json"
            }
        )
        response.raise_for_status()
        return response.json()