import requests

from macroeconomy.sources.datasource import DataSource

from macroeconomy.utils.constants import FRED_API_KEY_SECRET, SECRET_SCOPE
from macroeconomy.utils.secrets import SecretManager


class FredSource(DataSource):

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(
        self,
        start_date=None,
        end_date=None
    ):
        self.api_key = SecretManager.get_secret(SECRET_SCOPE, FRED_API_KEY_SECRET)
        self.start_date = start_date
        self.end_date = end_date

    @property
    def config_key(self):
        return "fred"

    def read(self, dataset):

        request_params = {
            "series_id": dataset,
            "api_key": self.api_key,
            "file_type": "json",
        }

        if self.start_date is None and self.end_date is None:
            # Último dato disponible
            request_params["sort_order"] = "desc"
            request_params["limit"] = 1

        else:
            if self.start_date is not None:
                request_params["observation_start"] = self.start_date

            if self.end_date is not None:
                request_params["observation_end"] = self.end_date

            request_params["sort_order"] = "asc"

        response = requests.get(
            self.BASE_URL,
            params=request_params,
            timeout=60
        )

        response.raise_for_status()

        return response.json()