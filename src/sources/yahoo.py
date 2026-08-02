from src.sources.datasource import DataSource
import yfinance as yf

class YahooFinanceSource(DataSource):

    def read(self, ticker, period="1d", interval="1d"):

        return yf.download(
            ticker,
            period=period,
            interval=interval
        )

