"""FeatureSet entity."""

from functools import reduce
from itertools import chain
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.core.transform import Feature


class FeatureSet:
    """Holds metadata about the feature set and constructs the dataframe."""

    def __init__(
        self,
        name: str,
        entity: str,
        description: str,
        features: List[Feature],
        key_columns: List[str],
        timestamp_column: str,
    ):
        """Initialize FeatureSet with specific configuration.

        :param name:  name of the feature set
        :param entity: business context tag for the feature set
        :param description: details about the feature set purpose
        :param features: features to compose the feature set
        :param key_columns: column names to be defined as keys.
        :param timestamp_column: column name to be defined as timestamp.
        """
        self.name = name
        self.entity = entity
        self.description = description
        self.features = features
        self.key_columns = key_columns
        self.timestamp_column = timestamp_column

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
            raise ValueError("name must be a string with the feature set label")
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
                "entity must be a string tagging the feature set business context"
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
                "description must be a string with the feature set details"
            )
        self.__description = value

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
            raise ValueError("features needs to be a list of Feature objects")
        self.__features = value

        # check duplicated column names
        if len(self.feature_set_columns) != len(set(self.feature_set_columns)):
            raise ValueError("features can't have output_columns with the same name")

    @property
    def feature_set_columns(self) -> List[str]:
        """Get all the output columns of al features.

        :return: flat list with all the feature columns
        """
        features_columns = [feature.get_output_columns() for feature in self.features]
        return list(chain.from_iterable(features_columns))  # flatten

    @property
    def key_columns(self) -> List[str]:
        """Attribute "key_columns" getter.

        :return key_columns: column names to be defined as keys.
        """
        return self.__key_columns

    @key_columns.setter
    def key_columns(self, value: List[str]):
        """Attribute "key_columns" setter.

        :param value: used to set attribute "key_columns".
        """
        if not isinstance(value, list):
            raise ValueError("key_columns must be a list of key column names")
        if not value or any(col not in self.feature_set_columns for col in value):
            raise ValueError(
                "Not all key_columns are in feature set, "
                "key_columns = {}, Features = {}".format(
                    value, self.feature_set_columns
                )
            )
        self.__key_columns = value

    @property
    def timestamp_column(self) -> str:
        """Attribute "timestamp_column" getter.

        :return timestamp_column: column name to be defined as timestamp.
        """
        return self.__timestamp_column

    @timestamp_column.setter
    def timestamp_column(self, value: str):
        """Attribute "timestamp_column" setter.

        :param value: used to set attribute "timestamp_column".
        """
        if value not in self.feature_set_columns:
            raise ValueError(
                "Timestamp column not found in feature set, "
                "timestamp_column = {}, Features = {}"
            )
        self.__timestamp_column = value

    def construct(self, input_df: DataFrame) -> DataFrame:
        """Use all the features to build the feature set dataframe.

        After that, there's the caching of the dataframe, however since cache() in
        Spark is lazy, an action is triggered in order to force persistence.

        :param input_df: input dataframe to be transformed by the features.
        :return: Spark dataframe with just the feature columns
        """
        if not isinstance(input_df, DataFrame):
            raise ValueError("source_df must be a dataframe")
        dataframe = reduce(
            lambda result_df, feature: feature.transform(result_df),
            self.features,
            input_df,
        ).select(*self.feature_set_columns)

        dataframe.cache().count()

        return dataframe
