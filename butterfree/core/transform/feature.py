"""Feature entity."""

from typing import List, Tuple, Union

from parameters_validation import non_blank
from pyspark.sql import DataFrame

from butterfree.core.transform.feature_component import FeatureComponent


class Feature(FeatureComponent):
    """Defines a Feature.

    Attributes:
        name: feature name.
        alias: new feature name, if necessary.
        origin: feature source.
        data_type: feature type.
        description: brief explanation regarding the feature.
    """

    def __init__(
        self,
        *,
        name: non_blank(str),
        alias: str = None,
        origin: Union[str, Tuple[str]],
        data_type: str = None,
        description: non_blank(str),
    ):
        self.name = (name,)
        self.alias = (alias,)
        self.origin = (origin,)
        self.description = (description,)
        self.data_type = (data_type,)
        self.transformations: List[FeatureComponent] = []

    def add(self, component: FeatureComponent):
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
            if self.alias[0]:
                return dataframe.withColumnRenamed(self.name[0], self.alias[0])
            return dataframe
        for transformation in self.transformations:
            return transformation.transform(dataframe)
