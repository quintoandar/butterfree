"""FeatureSet entity."""
import itertools
from functools import reduce
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature


class FeatureSet:
    """Holds metadata about the feature set and constructs the dataframe."""

    def __init__(
        self,
        name: str,
        entity: str,
        description: str,
        keys: List[KeyFeature],
        timestamp: TimestampFeature,
        features: List[Feature],
    ):
        """Initialize FeatureSet with specific configuration.

        :param name:  name of the feature set
        :param entity: business context tag for the feature set
        :param description: details about the feature set purpose
        :param features: features to compose the feature set
        """
        self.name = name
        self.entity = entity
        self.description = description
        self.keys = keys
        self.timestamp = timestamp
        self.features = features

    @property
    def name(self) -> str:
        """Attribute "name" getter.

        :return name: name of the feature set
        """
        return self.__name

    @name.setter
    def name(self, value: str):
        """Attribute "name" setter.

        :param value: used to set attribute "name".
        """
        if not isinstance(value, str):
            raise ValueError("name must be a string with the feature set label.")
        self.__name = value

    @property
    def entity(self) -> str:
        """Attribute "entity" getter.

        :return entity: business context tag for the feature set
        """
        return self.__entity

    @entity.setter
    def entity(self, value: str):
        """Attribute "entity" setter.

        :param value: used to set attribute "entity".
        """
        if not isinstance(value, str):
            raise ValueError(
                "entity must be a string tagging the feature set business context."
            )
        self.__entity = value

    @property
    def description(self) -> str:
        """Attribute "description" getter.

        :return description: details about the feature set purpose
        """
        return self.__description

    @description.setter
    def description(self, value: str):
        """Attribute "description" setter.

        :param value: used to set attribute "description".
        """
        if not isinstance(value, str):
            raise ValueError(
                "description must be a string with the feature set details."
            )
        self.__description = value

    @property
    def keys(self):
        return self.__keys

    @keys.setter
    def keys(self, value):
        if not isinstance(value, list) or not all(
            isinstance(item, KeyFeature) for item in value
        ):
            raise ValueError("keys needs to be a list of KeyFeature objects.")

        key_columns = list(itertools.chain(*[v.get_output_columns() for v in value]))
        if len(key_columns) != len(set(key_columns)):
            raise KeyError("key columns will have duplicates.")

        self.__keys = value

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, value):
        if not isinstance(value, TimestampFeature):
            raise ValueError("timestamp needs to be a TimestampFeature object.")

        timestamp_columns = value.get_output_columns()
        if len(timestamp_columns) > 1:
            raise ValueError("TimestampFeature will produce multiple output columns.")

        self.__timestamp = value

    @property
    def features(self) -> List[Feature]:
        """Attribute "features" getter.

        :return features: features to compose the feature set
        """
        return self.__features

    @features.setter
    def features(self, value: List[Feature]):
        """Attribute "features" setter.

        :param value: used to set attribute "features".
        """
        if not isinstance(value, list) or not all(
            isinstance(item, Feature) for item in value
        ):
            raise ValueError("features needs to be a list of Feature objects.")

        feature_columns = list(
            itertools.chain(*[v.get_output_columns() for v in value])
        )
        if len(feature_columns) != len(set(feature_columns)):
            raise KeyError("feature columns will have duplicates.")

        self.__features = value

    @property
    def columns(self):
        return list(
            itertools.chain(
                *[
                    k.get_output_columns()
                    for k in self.keys + [self.timestamp] + self.features
                ]
            )
        )

    def construct(self, dataframe: DataFrame) -> DataFrame:
        """Use all the features to build the feature set dataframe.

        After that, there's the caching of the dataframe, however since cache() in
        Spark is lazy, an action is triggered in order to force persistence.

        :param dataframe: input dataframe to be transformed by the features.
        :return: Spark dataframe with just the feature columns
        """
        if not isinstance(dataframe, DataFrame):
            raise ValueError("source_df must be a dataframe")
        output_df = reduce(
            lambda df, feature: feature.transform(df), self.features, dataframe,
        ).select(*self.columns)

        output_df.cache().count()

        return output_df
