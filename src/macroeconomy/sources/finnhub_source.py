import json
import websocket
from macroeconomy.sources.datasource import DataSource


class FinnhubSource(DataSource):

    BASE_URL = "wss://ws.finnhub.io"

    def __init__(self, api_key):
        self.api_key = api_key
        self.ws = None

    @property
    def config_key(self):
        return "finnhub"

    def read(self, dataset, on_data):

        socket = f"{self.BASE_URL}?token={self.api_key}"

        def on_open(ws):
            print("Conectado a Finnhub")

            for symbol in dataset:
                ws.send(json.dumps({
                    "type": "subscribe",
                    "symbol": symbol
                }))

        def on_message(ws, message):
            payload = json.loads(message)

            if payload.get("type") == "trade":
                on_data(payload)

        def on_error(ws, error):
            print("Error:", error)

        def on_close(ws, close_status_code, close_msg):
            print("Conexión cerrada")

        self.ws = websocket.WebSocketApp(
            socket,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        self.ws.run_forever()

    def close(self):
        if self.ws:
            self.ws.close()