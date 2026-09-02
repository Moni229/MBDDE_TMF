"""Fuente WebSocket de Finnhub que reenvía trades a Kafka."""

import json

import websocket
from confluent_kafka import Producer

from macroeconomy.sources.datasource import DataSource
from macroeconomy.utils.constants import (
    FINNHUB_API_KEY_SECRET,
    FINNHUB_CONFIG_KEY,
    FINNHUB_TRADES_TOPIC,
    SECRET_SCOPE,
)
from macroeconomy.utils.config import load_confluent_config
from macroeconomy.utils.secrets import SecretManager


class FinnhubSource(DataSource):
    """Se suscribe a trades de Finnhub y los publica en Kafka."""

    BASE_URL = "wss://ws.finnhub.io"
    TOPIC = FINNHUB_TRADES_TOPIC

    def __init__(self):
        self.api_key = SecretManager.get_secret(SECRET_SCOPE, FINNHUB_API_KEY_SECRET)
        self.kafka_config = load_confluent_config()
        self.ws = None
        self.producer = None

    @property
    def config_key(self):
        return FINNHUB_CONFIG_KEY

    @property
    def is_streaming(self):
        return True

    def _create_producer(self):
        producer_config = {
            "bootstrap.servers": self.kafka_config["bootstrap.servers"],
            "security.protocol": self.kafka_config["security.protocol"],
            "sasl.mechanism": self.kafka_config["sasl.mechanisms"],
            "sasl.username": self.kafka_config["sasl.username"],
            "sasl.password": self.kafka_config["sasl.password"],
        }

        return Producer(producer_config)

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"Error enviando mensaje a Kafka: {err}")
        else:
            print(
                "Mensaje enviado a Kafka: "
                f"topic={msg.topic()}, "
                f"partition={msg.partition()}, "
                f"offset={msg.offset()}"
            )

    def read(self, dataset, on_data=None):
        """Emite trades de Finnhub para uno o varios símbolos."""
        symbols = [dataset] if isinstance(dataset, str) else dataset

        self.producer = self._create_producer()
        socket_url = f"{self.BASE_URL}?token={self.api_key}"

        def on_open(ws):
            print(f"Conectado a Finnhub. Símbolos: {symbols}")

            for symbol in symbols:
                print(f"Suscribiendo a {symbol}")
                ws.send(
                    json.dumps(
                        {
                            "type": "subscribe",
                            "symbol": symbol,
                        }
                    )
                )

        def on_message(ws, message):
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                print(f"Mensaje JSON inválido: {message}")
                return

            message_type = payload.get("type")

            if message_type == "trade":
                try:
                    self.producer.produce(
                        topic=self.TOPIC,
                        value=json.dumps(payload).encode("utf-8"),
                        callback=self._delivery_report,
                    )
                    self.producer.poll(0)
                except BufferError:
                    print(
                        "Buffer de Kafka lleno. Esperando a que se entreguen mensajes..."
                    )
                    self.producer.poll(1)
                except Exception as e:
                    print(f"Error enviando trade a Kafka: {e}")

                if on_data is not None:
                    on_data(payload)
            elif message_type == "ping":
                print("Ping recibido de Finnhub")
            else:
                print(f"Mensaje Finnhub ignorado: {payload}")

        def on_error(ws, error):
            print(f"Error Finnhub: {error}")

        def on_close(ws, close_status_code, close_msg):
            print(f"Conexión Finnhub cerrada: {close_status_code} - {close_msg}")

            if self.producer is not None:
                self.producer.flush()

        self.ws = websocket.WebSocketApp(
            socket_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        print(f"Iniciando streaming Finnhub → Kafka ({self.TOPIC})")
        self.ws.run_forever()

    def close(self):
        if self.ws is not None:
            print("Cerrando conexión con Finnhub...")
            self.ws.close()
            self.ws = None

        if self.producer is not None:
            print("Cerrando Kafka Producer...")
            self.producer.flush()
            self.producer = None
