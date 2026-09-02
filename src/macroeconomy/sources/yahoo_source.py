"""Fuente de Yahoo Finance para datos históricos de mercado."""

import pandas as pd
import yfinance as yf

from macroeconomy.sources.datasource import DataSource
from macroeconomy.utils.constants import YAHOO_CONFIG_KEY


class YahooFinanceSource(DataSource):
    """Obtiene histórico de precios desde Yahoo Finance."""

    def __init__(
        self,
        start_date=None,
        end_date=None,
        interval="1d",
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval

    @property
    def config_key(self):
        return YAHOO_CONFIG_KEY

    def read(self, dataset):
        if self.start_date is None and self.end_date is None:
            df = yf.download(
                dataset,
                period="1d",
                interval=self.interval,
                auto_adjust=False,
                progress=False,
            )
        else:
            df = yf.download(
                dataset,
                start=self.start_date,
                end=self.end_date,
                interval=self.interval,
                auto_adjust=False,
                progress=False,
            )

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df.columns = [column.replace(" ", "_") for column in df.columns]
        return df.reset_index()
