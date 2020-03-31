"""AggregatedFeatureSet entity."""
from datetime import datetime
from functools import reduce
from typing import List

from pyspark.sql import DataFrame, functions

from butterfree.core.clients import SparkClient
from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
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

    @staticmethod
    def _generate_date_sequence_df(client: SparkClient, date_range, step=None):
        """Generates a date sequence dataframe from a given date range.

        Create a Spark DataFrame with a single column named timestamp and a
        range of dates within the desired interval (start and end dates included).
        It's also possible to provide a step argument.

        Attributes:
            client:  spark client used to create the dataframe.
            date_range: list of the desired date interval.
            step: time step.
        """
        day_in_seconds = 60 * 60 * 24
        start_date, end_date = date_range
        step = step or day_in_seconds
        date_df = client.conn.createDataFrame(
            [(start_date, end_date)], ("start_date", "end_date")
        ).select(
            [
                functions.col(c)
                .cast(DataType.TIMESTAMP.spark)
                .cast(DataType.BIGINT.spark)
                for c in ("start_date", "end_date")
            ]
        )
        start_date, end_date = date_df.first()
        return client.conn.range(start_date, end_date + day_in_seconds, step).select(
            functions.col("id").cast(DataType.TIMESTAMP.spark).alias(TIMESTAMP_COLUMN)
        )

    def _get_unique_keys(self, output_df):
        """Get key columns unique values.

        Create a Spark DataFrame with unique key columns values.

        Attributes:
            output_df:  dataframe to get unique key values.
        """
        return output_df.dropDuplicates(subset=self.keys_columns).select(
            self.keys_columns
        )

    @staticmethod
    def _set_cross_join_confs(client, state):
        """Defines spark configuration."""
        client.conn.conf.set("spark.sql.crossJoin.enabled", state)
        client.conn.conf.set("spark.sql.autoBroadcastJoinThreshold", int(state))

    def _cross_join_df(self, client: SparkClient, first_df, second_df):
        """Cross join between desired dataframes.

        Returns a cross join between the date daframe and transformed
        dataframe. In order to make this operation faster, the smaller
        dataframe is cached and autoBroadcastJoinThreshold conf is
        setted to False.

        Attributes:
            client:  spark client used to create the dataframe.
            first_df: a generic dataframe.
            second_df: another generic dataframe.
        """
        self._set_cross_join_confs(client, True)
        first_df.cache().take(
            1
        ) if second_df.count() >= first_df.count() else second_df.cache().take(1)
        cross_join_df = first_df.join(second_df)
        cross_join_df.cache().take(1)
        self._set_cross_join_confs(client, False)
        return cross_join_df

    def _create_date_range_dataframe(self, client: SparkClient, dataframe, end_date):
        """Performs a join between dataframes.

        Performs a join between the transformed dataframe and the date
        dataframe.

        Attributes:
            dataframe:  source dataframe.
            output_df: transformed dataframe.
            client: client responsible for connecting to Spark session.
        """
        start_date = dataframe.select(functions.min(TIMESTAMP_COLUMN)).collect()[0][0]
        end_date = end_date or datetime.now()
        date_range = [
            start_date
            if isinstance(start_date, str)
            else start_date.strftime("%Y-%m-%d"),
            end_date if isinstance(end_date, str) else end_date.strftime("%Y-%m-%d"),
        ]
        date_df = self._generate_date_sequence_df(client, date_range)
        unique_df = self._get_unique_keys(dataframe)
        cross_join_df = self._cross_join_df(client, date_df, unique_df)

        return cross_join_df

    def _dataframe_join(self, left, right, on, how):
        return left.join(right, on=on, how=how)

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
            base_date: user defined base date.

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
            range_df = self._create_date_range_dataframe(client, output_df, end_date)
            output_df = reduce(
                lambda left, right: self._dataframe_join(
                    left,
                    right,
                    on=self.keys_columns + [self.timestamp_column],
                    how="left",
                ),
                df_list,
                range_df,
            )

        elif self._has_aggregated_transform_without_window_only(self.features):
            agg_df = output_df.sort(self.timestamp_column).select(
                *[
                    functions.last(column).alias(column)
                    for column in self.keys_columns + [self.timestamp_column]
                ]
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
