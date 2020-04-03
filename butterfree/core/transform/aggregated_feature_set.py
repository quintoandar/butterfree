"""AggregatedFeatureSet entity."""
from functools import reduce
from typing import List

from pyspark.sql import DataFrame, functions

from butterfree.core.clients import SparkClient
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import AggregatedTransform


class AggregatedFeatureSet(FeatureSet):
    """Holds metadata about the aggregated feature set.

    This class overrides some methods of the ancestor FeatureSet class
    and has specific methods for aggregations.

    The AggregatedTransform can only be used on AggregatedFeatureSets.

    Example:
        This an example regarding the aggregated feature set definition. All features
        and its transformations are defined.
    >>> from butterfree.core.transform.aggregated_feature_set import (
    ...       AggregatedFeatureSet
    ... )
    >>> from butterfree.core.transform.features import (
    ...     Feature,
    ...     KeyFeature,
    ...     TimestampFeature,
    ...)
    >>> from butterfree.core.transform.transformations import (
    ...     AggregatedTransform,
    ... )
    >>> feature_set = AggregatedFeatureSet(
    ...    name="aggregated feature_set",
    ...    entity="entity",
    ...    description="description",
    ...    features=[
    ...        Feature(
    ...            name="feature1",
    ...            description="test",
    ...            transformation=AggregatedTransform(
    ...                 functions=["avg", "stddev_pop"],
    ...                 group_by="id",
    ...                 column="feature1",
    ...             ).with_window(window_definition=["1 day"],),
    ...        ),
    ...    ],
    ...    keys=[KeyFeature(name="id", description="The user's Main ID or device ID")],
    ...    timestamp=TimestampFeature(),
    ...)
    >>> feature_set.construct(dataframe=dataframe)

    The construct method will execute the feature set, computing all the
    defined aggregated transformations.

    When you use an AggregatedFeatureSet without window, we defined that
    the TimestampFeature is the lastest time value in the column.
    """

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

        feature_columns = self._get_features_columns(*value)
        if len(feature_columns) != len(set(feature_columns)):
            raise KeyError("feature columns will have duplicates.")

        if not self._has_aggregated_transform_only(value):
            raise ValueError(
                "You can't define a aggregated feature without aggregated transform. "
                "You need to use Feature Set."
            )

        if not self._has_aggregated_transform_with_window_only(
            value
        ) and not self._has_aggregated_transform_without_window_only(value):
            raise ValueError(
                "You can only define aggregate transformations with "
                "or without windows, so you cannot "
                "define with both types."
            )

        self.__features = value

    @staticmethod
    def _has_aggregated_transform_only(features):
        """Aggregated Transform check.

        Checks if all transformations are AggregatedTransform within the scope of the
        AggregatedFeatureSet.

        Returns:
            True if there's a aggregation transform.

        """
        return all(
            [
                isinstance(feature.transformation, AggregatedTransform)
                for feature in features
            ]
        )

    @staticmethod
    def _has_aggregated_transform_with_window_only(features):
        """Aggregated Transform window check.

        Checks if there's a window within the scope of the
        all AggregatedTransform.

        Returns:
            True if there's a window in all features.

        """
        return all([feature.transformation.has_windows for feature in features])

    @staticmethod
    def _has_aggregated_transform_without_window_only(features):
        """Aggregated Transform without window check.

        Checks if there isn't a window within the scope of the
        all AggregatedTransform.

        Returns:
            True if there isn't a window in all features.

        """
        return all([not feature.transformation.has_windows for feature in features])

    def _dataframe_join(self, left, right, on, how):
        return left.join(right, on=on, how=how)

    def _create_agg_df_list(self, dataframe):
        """Returns two aggregated dataframes.

        The first dataframe consists of a date prior to the minimum
        source dataframe date. The other one is the date next to the
        maximun source dataframe date.

        Attributes:
            dataframe:  dataframe to be aggregated.
        """
        agg_list = []
        for spec in [
            (functions.min, functions.date_sub),
            (functions.max, functions.date_add),
        ]:
            agg_df = (
                dataframe.groupBy(self.keys_columns)
                .agg(spec[0](self.timestamp_column).alias(self.timestamp_column))
                .withColumn(self.timestamp_column, spec[1](self.timestamp_column, 1))
            )
            agg_list.append(agg_df)
        return agg_list

    def construct(
        self, dataframe: DataFrame, client: SparkClient, end_date: str = None
    ) -> DataFrame:
        """Use all the features to build the feature set dataframe.

        After that, there's the caching of the dataframe, however since cache()
        in Spark is lazy, an action is triggered in order to force persistence,
        but we only cache if it is not a streaming spark dataframe.

        Args:
            dataframe: input dataframe to be transformed by the features.
            client: client responsible for connecting to Spark session.
            end_date: user defined base date.

        Returns:
            Spark dataframe with all the feature columns.

        """
        df_list = []
        if not isinstance(dataframe, DataFrame):
            raise ValueError("source_df must be a dataframe")

        output_df = reduce(
            lambda df, feature: feature.transform(df),
            self.keys + [self.timestamp],
            dataframe,
        )

        for feature in self.features:
            feature_df = feature.transform(output_df)
            df_list.append(feature_df)

        if self._has_aggregated_transform_with_window_only(self.features):

            output_df = reduce(
                lambda left, right: self._dataframe_join(
                    left,
                    right,
                    on=self.keys_columns + [self.timestamp_column],
                    how="full_outer",
                ),
                df_list,
            )

            agg_df_list = self._create_agg_df_list(output_df)

            output_df = reduce(
                lambda left, right: self._dataframe_join(
                    left,
                    right,
                    on=self.keys_columns + [self.timestamp_column],
                    how="full_outer",
                ),
                agg_df_list,
                output_df,
            )

            if end_date:
                output_df = output_df.filter(f"{self.timestamp_column} <= '{end_date}'")

        elif self._has_aggregated_transform_without_window_only(self.features):
            agg_df = output_df.groupBy(self.keys_columns).agg(
                functions.max(functions.col(self.timestamp_column)).alias(
                    self.timestamp_column
                )
            )
            output_df = reduce(
                lambda left, right: self._dataframe_join(
                    left,
                    right,
                    on=self.features[0].transformation.group_by,
                    how="full_outer",
                ),
                df_list,
                agg_df,
            )

        output_df = output_df.select(*self.columns)

        if not output_df.isStreaming:
            output_df = self._filter_duplicated_rows(output_df)
            output_df.cache().count()

        return output_df
