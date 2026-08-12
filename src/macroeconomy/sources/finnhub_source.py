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

    @property
    def is_streaming(self):
        return True

    def read(self, dataset, on_data):
        """
        Consume datos de Finnhub mediante WebSocket.

        Parameters
        ----------
        dataset : str | list[str]
            Símbolo o símbolos a suscribir.

        on_data : callable
            Callback que recibe cada mensaje de Finnhub.
        """

        symbols = [dataset] if isinstance(dataset, str) else dataset

        socket_url = f"{self.BASE_URL}?token={self.api_key}"

        def on_open(ws):
            print(f"Conectado a Finnhub: {symbols}")

            for symbol in symbols:
                print(f"Suscribiendo a {symbol}")

                ws.send(json.dumps({
                    "type": "subscribe",
                    "symbol": symbol
                }))

        def on_message(ws, message):
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                print(f"Mensaje JSON inválido: {message}")
                return

            message_type = payload.get("type")

            if message_type == "trade":
                on_data(payload)

            elif message_type == "ping":
                # Finnhub puede enviar mensajes de mantenimiento
                print("Ping recibido de Finnhub")

            else:
                print(f"Mensaje Finnhub ignorado: {payload}")

        def on_error(ws, error):
            print(f"Error Finnhub: {error}")

        def on_close(ws, close_status_code, close_msg):
            print(
                f"Conexión Finnhub cerrada: "
                f"{close_status_code} - {close_msg}"
            )

        self.ws = websocket.WebSocketApp(
            socket_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        # Esta llamada bloquea y mantiene el streaming activo.
        self.ws.run_forever()

    def close(self):
        if self.ws is not None:
            print("Cerrando conexión con Finnhub...")
            self.ws.close()
            self.ws = None