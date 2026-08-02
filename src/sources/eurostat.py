from src.sources.datasource import DataSource
import requests

class EurostatSource(DataSource):

    def __init__(self, base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"):
        self.base_url = base_url

    def read(self, dataset, time=None):
        url = f"{self.base_url}/{dataset}"
        params = {
            "geo": "EA20",
            "coicop": "CP00",
            "unit": "I15"
        }

        if time is not None:
            params["time"] = time

        response = requests.get(url, params=params)
        response.raise_for_status()

        return response.json()
