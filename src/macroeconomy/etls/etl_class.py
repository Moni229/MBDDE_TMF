from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class ETLClass(ABC):

    @abstractmethod
    def transform(self, *args, **kwargs) -> DataFrame:
        pass