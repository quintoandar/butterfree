"""Feature entity."""

from typing import List

from pyspark.sql import DataFrame

from butterfree.core.transform.transform_component import TransformComponent


class Feature:
    """Defines a Feature.

    Attributes:
        name: feature name.
        alias: new feature name, if necessary.
        description: brief explanation regarding the feature.
    """

    def __init__(
        self, *, name: str, alias: str = None, description: str,
    ):
        self.name = name
        self.alias = alias
        self.description = description
        self.transformations: List[TransformComponent] = []

    def add(self, component: TransformComponent):
        """Adds new component to the feature pipeline.

        Args:
            component: desired component.

        Returns:
            component.parent: component from parent class.
        """
        self.transformations.append(component)
        component.parent = self
        return self

    def transform(self, dataframe: DataFrame):
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: base dataframe.

        Returns:
            dataframe: transformed dataframe.
        """
        if not self.transformations:
            return dataframe.withColumnRenamed(
                self.name, self.alias if self.alias else self.name
            )
        for transformation in self.transformations:
            return transformation.transform(dataframe)
