import json
import websocket
from confluent_kafka import Producer
from macroeconomy.sources.datasource import DataSource


class FinnhubSource(DataSource):

    BASE_URL = "wss://ws.finnhub.io"
    TOPIC = "finnhub_trades"

    def __init__(self, api_key, kafka_config):
        self.api_key = api_key
        self.kafka_config = kafka_config

        self.ws = None
        self.producer = None

    @property
    def config_key(self):
        return "finnhub"

    @property
    def is_streaming(self):
        return True

    def _create_producer(self):
        """
        Crea el Producer de Confluent Kafka.
        """

        producer_config = {
            "bootstrap.servers": self.kafka_config["bootstrap.servers"],
            "security.protocol": self.kafka_config["security.protocol"],
            "sasl.mechanism": self.kafka_config["sasl.mechanism"],
            "sasl.username": self.kafka_config["sasl.username"],
            "sasl.password": self.kafka_config["sasl.password"],
        }

        return Producer(producer_config)

    def _delivery_report(self, err, msg):
        """
        Callback ejecutado cuando Kafka confirma o rechaza
        la entrega de un mensaje.
        """

        if err is not None:
            print(
                f"Error enviando mensaje a Kafka: "
                f"{err}"
            )
        else:
            print(
                f"Mensaje enviado a Kafka: "
                f"topic={msg.topic()}, "
                f"partition={msg.partition()}, "
                f"offset={msg.offset()}"
            )

    def read(self, dataset, on_data=None):
        """
        Consume trades de Finnhub mediante WebSocket y los
        publica en Kafka.

        Parameters
        ----------
        dataset : str | list[str]
            Símbolo o símbolos a suscribir.

        on_data : callable, optional
            Callback adicional que recibe el mensaje original
            de Finnhub.
        """

        symbols = (
            [dataset]
            if isinstance(dataset, str)
            else dataset
        )

        # Crear Kafka Producer
        self.producer = self._create_producer()

        socket_url = f"{self.BASE_URL}?token={self.api_key}"

        def on_open(ws):
            print(
                f"Conectado a Finnhub. "
                f"Símbolos: {symbols}"
            )

            for symbol in symbols:

                print(
                    f"Suscribiendo a {symbol}"
                )

                ws.send(
                    json.dumps({
                        "type": "subscribe",
                        "symbol": symbol
                    })
                )

        def on_message(ws, message):

            try:
                payload = json.loads(message)

            except json.JSONDecodeError:
                print(
                    f"Mensaje JSON inválido: "
                    f"{message}"
                )
                return

            message_type = payload.get("type")

            # Solo procesamos trades
            if message_type == "trade":

                try:
                    # Publicamos el mensaje ORIGINAL
                    # de Finnhub sin modificarlo.
                    self.producer.produce(
                        topic=self.TOPIC,
                        value=json.dumps(payload).encode("utf-8"),
                        callback=self._delivery_report,
                    )

                    # Sirve para que confluent_kafka procese
                    # callbacks y entregue mensajes pendientes.
                    self.producer.poll(0)

                except BufferError:
                    print(
                        "Buffer de Kafka lleno. "
                        "Esperando a que se entreguen mensajes..."
                    )

                    self.producer.poll(1)

                except Exception as e:
                    print(
                        f"Error enviando trade a Kafka: {e}"
                    )

                # Mantener el callback original
                if on_data is not None:
                    on_data(payload)

            elif message_type == "ping":

                print(
                    "Ping recibido de Finnhub"
                )

            else:

                print(
                    f"Mensaje Finnhub ignorado: "
                    f"{payload}"
                )

        def on_error(ws, error):

            print(
                f"Error Finnhub: {error}"
            )

        def on_close(
            ws,
            close_status_code,
            close_msg
        ):

            print(
                f"Conexión Finnhub cerrada: "
                f"{close_status_code} - {close_msg}"
            )

            # Asegurar que los mensajes pendientes
            # se entregan antes de cerrar.
            if self.producer is not None:
                self.producer.flush()

        self.ws = websocket.WebSocketApp(
            socket_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        print(
            f"Iniciando streaming Finnhub → "
            f"Kafka ({self.TOPIC})"
        )

        # Mantiene el WebSocket activo
        self.ws.run_forever()

    def close(self):

        if self.ws is not None:

            print(
                "Cerrando conexión con Finnhub..."
            )

            self.ws.close()
            self.ws = None

        if self.producer is not None:

            print(
                "Cerrando Kafka Producer..."
            )

            # Esperar a que se entreguen
            # todos los mensajes pendientes.
            self.producer.flush()

            self.producer = None