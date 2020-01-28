"""CustomTransform entity."""

from typing import Callable

from pyspark.sql import DataFrame

from butterfree.core.transform.transform_component import TransformComponent


class CustomTransform(TransformComponent):
    """Defines a Custom Transform.

    Attributes:
        transformer: custom transformer.
    """

    def __init__(self, transformer: Callable, **kwargs):
        super().__init__()
        self.transformer = transformer
        self.transformer__kwargs = kwargs

    @property
    def transformer(self):
        """Returns the transformer."""
        return self._transformer

    @transformer.setter
    def transformer(self, method: Callable):
        if not method:
            raise ValueError("A method must be provided to CustomTransform")
        self._transformer = method

    def transform(self, dataframe: DataFrame, **kwargs):
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: base dataframe.

        Returns:
            dataframe: transformed dataframe.
        """
        dataframe = self.transformer(
            dataframe,
            self._parent.alias if self._parent.alias else self._parent.name,
            **self.transformer__kwargs,
        )
        return dataframe
