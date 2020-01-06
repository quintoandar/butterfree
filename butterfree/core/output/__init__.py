from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class Output(ABC):
    @abstractmethod
    def write(self, dataframe: DataFrame):
        pass
