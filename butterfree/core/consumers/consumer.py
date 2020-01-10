from abc import ABC, abstractmethod


class Consumer(ABC):
    """
    Abstract base class for database consumers.
    """

    @abstractmethod
    def consume(self, options):
        """
        Gets all data from a table.
        :param options: Consumer options to read the data
        :return: A DataFrame with the desired data
        """
        pass
