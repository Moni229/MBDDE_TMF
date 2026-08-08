from macroeconomy.sources.datasource import DataSource
import yfinance as yf
import pandas as pd

class YahooFinanceSource(DataSource):

    @property
    def config_key(self):
        return "yahoo"

    def read(self, dataset, period="1d", interval="1d"):

        df = yf.download(
            dataset,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
        )
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df.columns = [
            column.replace(" ", "_")
            for column in df.columns
        ]

        return df.reset_index()
