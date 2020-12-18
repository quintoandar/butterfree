from abc import ABCMeta, abstractmethod

from pyspark.sql import Row


class ForEachWriter(metaclass=ABCMeta):
    @abstractmethod
    def open(self, partition_id: str, epoch_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def process(self, row: Row) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self, error: Exception) -> None:
        raise NotImplementedError
