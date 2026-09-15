"""Clases base para fuentes de ingesta y tareas de carga."""

from abc import ABC, abstractmethod

from macroeconomy.utils.constants import DATASOURCE_CONFIG_KEY


class DataSource(ABC):
    """Interfaz común para fuentes batch y streaming."""

    @property
    def config_key(self):
        return DATASOURCE_CONFIG_KEY

    @property
    def is_streaming(self):
        return False

    @abstractmethod
    def read(self, dataset, **kwargs):
        """Lee datos desde la fuente."""
        pass


class IngestionJob:
    """Orquesta la lectura de una fuente y su persistencia."""

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def run(self, source, dataset, **kwargs):
        data = self.reader.read(**kwargs)
        self.writer.write(data=data, source=source, dataset=dataset)
