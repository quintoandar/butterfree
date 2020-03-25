"""FeatureSet entity."""
import itertools
from datetime import datetime
from functools import reduce
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from butterfree.core.clients import SparkClient
from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import AggregatedTransform


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

    Example:
        This an example regarding the feature set definition. All features
        and its transformations are defined.
    >>> from butterfree.core.transform import FeatureSet
    >>> from butterfree.core.transform.features import (
    ...     Feature,
    ...     KeyFeature,
    ...     TimestampFeature,
    ...)
    >>> from butterfree.core.transform.transformations import (
    ...     AggregatedTransform,
    ...     CustomTransform,
    ... )
    >>> import pyspark.sql.functions as F

    >>> def divide(df, fs, column1, column2):
    ...     name = fs.get_output_columns()[0]
    ...     df = df.withColumn(name, F.col(column1) / F.col(column2))
    ...     return df

    >>> feature_set = FeatureSet(
    ...    name="feature_set",
    ...    entity="entity",
    ...    description="description",
    ...    features=[
    ...        Feature(
    ...            name="feature1",
    ...            description="test",
    ...            transformation=AggregatedTransform(
    ...                aggregations=["avg", "std"],
    ...                partition="id",
    ...                windows=["2 minutes", "15 minutes"],
    ...            ),
    ...        ),
    ...        Feature(
    ...            name="divided_feature",
    ...            description="unit test",
    ...            transformation=CustomTransform(
    ...                transformer=divide, column1="feature1", column2="feature2",
    ...            ),
    ...        ),
    ...    ],
    ...    keys=[KeyFeature(name="id", description="The user's Main ID or device ID")],
    ...    timestamp=TimestampFeature(),
    ...)

    >>> feature_set.construct(dataframe=dataframe)

    This last method (construct) will execute the feature set, computing all the
    defined transformations.

    There's also a functionality regarding the construct method within the scope
    of FeatureSet called filter_duplicated_rows. We drop rows that have repeated
    values over key columns and timestamp column, we do this in order to reduce
    our dataframe (regarding the number of rows). A detailed explation of this
    method can be found at filter_duplicated_rows docstring.
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

    @staticmethod
    def _get_features_columns(*features) -> List[str]:
        return list(itertools.chain(*[k.get_output_columns() for k in features]))

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

        key_columns = self._get_features_columns(*value)
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

        feature_columns = self._get_features_columns(*value)
        if len(feature_columns) != len(set(feature_columns)):
            raise KeyError("feature columns will have duplicates.")

        if self._has_rolling_windows_only(value) and len(value) > 1:
            raise ValueError(
                "You can define only one feature within the scope of the "
                "rolling windows aggregated transform, since the output "
                "dataframe will only contain features related to this "
                "transformation."
            )

        self.__features = value

    @property
    def keys_columns(self) -> List[str]:
        """Name of the columns of all keys in feature set."""
        return self._get_features_columns(*self.keys)

    @property
    def timestamp_column(self) -> str:
        """Name of the timestamp column in feature set."""
        return self._get_features_columns(self.timestamp).pop()

    @property
    def features_columns(self) -> List[str]:
        """Name of the columns of all features in feature set."""
        return self._get_features_columns(*self.features)

    @property
    def columns(self) -> List[str]:
        """All data columns within this feature set.

        This references all data columns that will be created by the construct
        method, given keys, timestamp and features of this feature set.

        Returns:
            List of column names built in this feature set.

        """
        return self.keys_columns + [self.timestamp_column] + self.features_columns

    @staticmethod
    def _has_rolling_windows_only(features):
        """Aggregated Transform mode check.

        Checks if there's a rolling window mode within the scope of the
        AggregatedTransform.

        Returns:
            True if there's a rolling window aggregation mode.

        """
        for feature in features:
            if isinstance(feature.transformation, AggregatedTransform) and (
                feature.transformation.mode[0] == "rolling_windows"
            ):
                return True
            return False

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
                F.col(c).cast(DataType.TIMESTAMP.spark_mapping).cast(DataType.BIGINT.spark_mapping)
                for c in ("start_date", "end_date")
            ]
        )
        start_date, end_date = date_df.first()
        return client.conn.range(start_date, end_date + day_in_seconds, step).select(
            F.col("id").cast(DataType.TIMESTAMP.spark_mapping).alias(TIMESTAMP_COLUMN)
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

    def _filter_duplicated_rows(self, df):
        """Filter dataframe duplicated rows.

        Attributes:
            df: transformed dataframe.

        Returns:
            Spark dataframe with filtered rows.

        Example:
            Suppose, for instance, that the transformed dataframe received
            by the construct method has the following rows:

            +---+----------+--------+--------+--------+
            | id| timestamp|feature1|feature2|feature3|
            +---+----------+--------+--------+--------+
            |  1|         1|       0|    null|       1|
            |  1|         2|       0|       1|       1|
            |  1|         3|    null|    null|    null|
            |  1|         4|       0|       1|       1|
            |  1|         5|       0|       1|       1|
            |  1|         6|    null|    null|    null|
            |  1|         7|    null|    null|    null|
            +---+-------------------+--------+--------+

            We will then create four columns, the first one, rn_by_key_columns
            (rn1) is the row number over a key columns partition ordered by timestamp.
            The second, rn_by_all_columns (rn2), is the row number over all columns
            partition (also ordered by timestamp). The third column,
            lag_rn_by_key_columns (lag_rn1), returns the last occurrence of the
            rn_by_key_columns over all columns partition. The last column, diff, is
            the difference between rn_by_key_columns and lag_rn_by_key_columns:

            +---+----------+--------+--------+--------+----+----+--------+-----+
            | id| timestamp|feature1|feature2|feature3| rn1| rn2| lag_rn1| diff|
            +---+----------+--------+--------+--------+----+----+--------+-----+
            |  1|         1|       0|    null|       1|   1|   1|    null| null|
            |  1|         2|       0|       1|       1|   2|   1|    null| null|
            |  1|         3|    null|    null|    null|   3|   1|    null| null|
            |  1|         4|       0|       1|       1|   4|   2|       2|    2|
            |  1|         5|       0|       1|       1|   5|   3|       4|    1|
            |  1|         6|    null|    null|    null|   6|   2|       3|    3|
            |  1|         7|    null|    null|    null|   7|   3|       6|    1|
            +---+----------+--------+--------+--------+----+----+--------+-----+

            Finally, this dataframe will then be filtered with the following condition:
            rn_by_all_columns = 1 or diff > 1 and only the original columns will be
            returned:

            +---+----------+--------+--------+--------+
            | id| timestamp|feature1|feature2|feature3|
            +---+----------+--------+--------+--------+
            |  1|         1|       0|    null|       1|
            |  1|         2|       0|       1|       1|
            |  1|         3|    null|    null|    null|
            |  1|         4|       0|       1|       1|
            |  1|         6|    null|    null|    null|
            +---+----------+--------+--------+--------+

        """
        window_key_columns = Window.partitionBy(self.keys_columns).orderBy(
            TIMESTAMP_COLUMN
        )
        window_all_columns = Window.partitionBy(
            self.keys_columns + self.features_columns
        ).orderBy(TIMESTAMP_COLUMN)

        df = (
            df.withColumn("rn_by_key_columns", F.row_number().over(window_key_columns))
            .withColumn("rn_by_all_columns", F.row_number().over(window_all_columns))
            .withColumn(
                "lag_rn_by_key_columns",
                F.lag("rn_by_key_columns", 1).over(window_all_columns),
            )
            .withColumn(
                "diff", F.col("rn_by_key_columns") - F.col("lag_rn_by_key_columns")
            )
        )
        df = df.filter("rn_by_all_columns = 1 or diff > 1")

        return df.select([column for column in self.columns])

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
        if not isinstance(dataframe, DataFrame):
            raise ValueError("source_df must be a dataframe")

        output_df = reduce(
            lambda df, feature: feature.transform(df),
            self.keys + [self.timestamp] + self.features,
            dataframe,
        ).select(*self.columns)

        if self._has_rolling_windows_only(self.features):
            output_df = self._rolling_window_joins(
                dataframe, output_df, client, base_date
            )

        if not output_df.isStreaming:
            output_df = self._filter_duplicated_rows(output_df)
            output_df.cache().count()

        return output_df
