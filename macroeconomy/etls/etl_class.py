from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class ETLClass(ABC):
    """Clase abstracta ETLClass, utilizada por todas las ETLs del repo"""
    @abstractmethod
    def transform(self, *args, **kwargs) -> DataFrame:
        pass
