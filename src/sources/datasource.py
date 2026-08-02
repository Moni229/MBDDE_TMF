from abc import ABC, abstractmethod


class DataSource(ABC):

    @abstractmethod
    def read(self, **kwargs):
        """Obtiene los datos desde la fuente."""
        pass

class IngestionJob:

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def run(self, source, dataset, **kwargs):
        data = self.reader.read(**kwargs)

        self.writer.write(
            data=data,
            source=source,
            dataset=dataset
        )