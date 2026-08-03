from macroeconomy.sources.datasource import DataSource
import yfinance as yf

class YahooFinanceSource(DataSource):

    @property
    def config_key(self):
        return "yahoo"

    def read(self, dataset, period="1d", interval="1d"):

        return yf.download(
            dataset,
            period=period,
            interval=interval
        )

