from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class SilverTransformer(ABC):

    @abstractmethod
    def transform(
        self,
        df: DataFrame,
    ) -> DataFrame:
        pass