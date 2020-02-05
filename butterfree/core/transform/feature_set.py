"""FeatureSet entity."""
import itertools
from functools import reduce
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature


class FeatureSet:
    """Holds metadata about the feature set and constructs the final dataframe.

    Attributes:
        name:  name of the feature set.
        entity: business context tag for the feature set, an entity for which we
            are creating all these features.
        description: details about the feature set purpose.
        keys: key features to define this feature set.
            Values for keys (may be a composition) should be unique on each
            moment in time (controlled by the TimestampFeature).
        timestamp: A single feature that define a timestamp for each observation
            in this feature set.
        features: features to compose the feature set.

    """

    def __init__(
        self,
        name: str,
        entity: str,
        description: str,
        keys: List[KeyFeature],
        timestamp: TimestampFeature,
        features: List[Feature],
    ) -> None:
        self.name = name
        self.entity = entity
        self.description = description
        self.keys = keys
        self.timestamp = timestamp
        self.features = features

    @property
    def name(self) -> str:
        """Name of the feature set."""
        return self.__name

    @name.setter
    def name(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("name must be a string with the feature set label.")
        self.__name = value

    @property
    def entity(self) -> str:
        """Business context tag for the feature set."""
        return self.__entity

    @entity.setter
    def entity(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError(
                "entity must be a string tagging the feature set business context."
            )
        self.__entity = value

    @property
    def description(self) -> str:
        """Details about the feature set purpose."""
        return self.__description

    @description.setter
    def description(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError(
                "description must be a string with the feature set details."
            )
        self.__description = value

    @property
    def keys(self) -> List[KeyFeature]:
        """Key features to define this feature set."""
        return self.__keys

    @keys.setter
    def keys(self, value: List[KeyFeature]) -> None:
        if not isinstance(value, list) or not all(
            isinstance(item, KeyFeature) for item in value
        ):
            raise ValueError("keys needs to be a list of KeyFeature objects.")

        key_columns = list(itertools.chain(*[v.get_output_columns() for v in value]))
        if len(key_columns) != len(set(key_columns)):
            raise KeyError("key columns will have duplicates.")

        self.__keys = value

    @property
    def timestamp(self) -> TimestampFeature:
        """Defines a timestamp for each observation in this feature set."""
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, value: TimestampFeature):
        if not isinstance(value, TimestampFeature):
            raise ValueError("timestamp needs to be a TimestampFeature object.")

        timestamp_columns = value.get_output_columns()
        if len(timestamp_columns) > 1:
            raise ValueError("TimestampFeature will produce multiple output columns.")

        self.__timestamp = value

    @property
    def features(self) -> List[Feature]:
        """Features to compose the feature set."""
        return self.__features

    @features.setter
    def features(self, value: List[Feature]):
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
    def columns(self) -> List[str]:
        """All data columns within this feature set.

        This references all data columns that will be created by the construct
        method, given keys, timestamp and features of this feature set.

        Returns:
            List of column names built in this feature set.

        """
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

        After that, there's the caching of the dataframe, however since cache()
        in Spark is lazy, an action is triggered in order to force persistence.

        Args:
            dataframe: input dataframe to be transformed by the features.

        Returns:
            Spark dataframe with all the feature columns.

        """
        if not isinstance(dataframe, DataFrame):
            raise ValueError("source_df must be a dataframe")
        output_df = reduce(
            lambda df, feature: feature.transform(df), self.features, dataframe,
        ).select(*self.columns)

        output_df.cache().count()

        return output_df
