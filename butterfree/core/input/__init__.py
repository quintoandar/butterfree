from abc import ABC, abstractmethod

from parameters_validation import validate_parameters, non_blank, no_whitespaces
from pyspark.sql import DataFrame


class Fetcher(ABC):
    @abstractmethod
    def fetch(self) -> DataFrame:
        pass


class Input(ABC):
    @abstractmethod
    def fetch(self) -> DataFrame:
        pass

    @validate_parameters
    def create_view(self, view_name: non_blank(no_whitespaces(str))):
        self.fetch().createTempView(view_name)
