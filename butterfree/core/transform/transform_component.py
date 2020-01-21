"""Transform Abstract Class."""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class TransformComponent(ABC):
    """Defines an abstract TransformComponent."""

    def __init__(self):
        self._parent = None

    @property
    def parent(self):
        """Returns the component parent."""
        return self._parent

    @parent.setter
    def parent(self, parent):
        self._parent = parent

    def _get_alias(self, alias):
        if alias is not None:
            return self._parent.alias
        else:
            return self._parent.name

    @abstractmethod
    def transform(self, dataframe: DataFrame):
        """Base transform method.

        Args:
            dataframe: base dataframe.

        Returns:
            dataframe: transformed dataframe.
        """
        pass
