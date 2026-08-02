import requests
from src.sources.datasource import DataSource


class FredSource(DataSource):

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key):
        self.api_key = api_key

    def read(self, series):

        response = requests.get(
            self.BASE_URL,
            params={
                "series_id": series,
                "api_key": self.api_key,
                "file_type": "json"
            }
        )
        response.raise_for_status()
        return response.json()