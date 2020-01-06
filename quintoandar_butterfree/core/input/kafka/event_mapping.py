from abc import ABC, abstractmethod


class EventMapping(ABC):
    @property
    @abstractmethod
    def topic(self):
        pass

    @property
    @abstractmethod
    def schema(self):
        pass

    filter_query = "SELECT * FROM dataframe"
