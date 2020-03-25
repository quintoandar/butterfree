"""AggregatedFeatureSet entity."""
from datetime import datetime
from functools import reduce
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from butterfree.core.clients import SparkClient
from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import AggregatedTransform


class AggregatedFeatureSet(FeatureSet):
    """Holds metadata about the aggregated feature set ."""

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

        self.__features = value

    @staticmethod
    def _has_aggregated_transform_only(features):
        """Aggregated Transform mode check.

        Checks if there's a rolling window mode within the scope of the
        AggregatedTransform.

        Returns:
            True if there's a rolling window aggregation mode.

        """
        for feature in features:
            if not isinstance(feature.transformation, AggregatedTransform):
                return False
        return True

    @staticmethod
    def _generate_dates(client: SparkClient, date_range, step=None):
        """Generate date dataframe.

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
                F.col(c).cast(DataType.TIMESTAMP.value).cast(DataType.BIGINT.value)
                for c in ("start_date", "end_date")
            ]
        )
        start_date, end_date = date_df.first()
        return client.conn.range(start_date, end_date + day_in_seconds, step).select(
            F.col("id").cast(DataType.TIMESTAMP.value).alias(TIMESTAMP_COLUMN)
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

    def _rolling_window_joins(
        self, dataframe, output_df, client: SparkClient, end_date
    ):
        """Performs a join between dataframes.

        Performs a join between the transformed dataframe and the date
        dataframe.

        Attributes:
            dataframe:  source dataframe.
            output_df: transformed dataframe.
            client: client responsible for connecting to Spark session.
        """
        start_date = dataframe.select(F.min(TIMESTAMP_COLUMN)).collect()[0][0]
        end_date = end_date or datetime.now()
        date_range = [
            start_date
            if isinstance(start_date, str)
            else start_date.strftime("%Y-%m-%d"),
            end_date if isinstance(end_date, str) else end_date.strftime("%Y-%m-%d"),
        ]
        date_df = self._generate_dates(client, date_range)
        unique_df = self._get_unique_keys(output_df)
        cross_join_df = self._cross_join_df(client, date_df, unique_df)
        output_df = cross_join_df.join(
            output_df, on=self.keys_columns + [self.timestamp_column], how="left"
        )
        return output_df

    def _dataframe_join(self, df_base, df):
        return df_base.join(
            df, on=self.keys_columns + [self.timestamp_column], how="full_outer"
        )

    def construct(
        self, dataframe: DataFrame, client: SparkClient, base_date: str = None
    ) -> DataFrame:
        """Use all the features to build the feature set dataframe.

        After that, there's the caching of the dataframe, however since cache()
        in Spark is lazy, an action is triggered in order to force persistence.

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

        for feature in self.keys + [self.timestamp] + self.features:
            feature_df = feature.transform(dataframe)
            if feature.transformation:
                if feature.transformation.with_window:
                    df_list.append(
                        self._rolling_window_joins(
                            dataframe, feature_df, client, base_date
                        )
                    )
                else:
                    df_list.append(feature_df)
            else:
                df_list.append(feature_df)

        output_df = reduce(self._dataframe_join, df_list).select(*self.columns)

        if not output_df.isStreaming:
            output_df = self._filter_duplicated_rows(output_df)
            output_df.cache().count()

        return output_df
