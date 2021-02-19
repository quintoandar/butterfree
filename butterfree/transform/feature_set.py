"""FeatureSet entity."""
import itertools
from functools import reduce
from typing import Any, Dict, List, Optional

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.dataframe import DataFrame

from butterfree.clients import SparkClient
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.dataframe_service import IncrementalStrategy
from butterfree.hooks import HookableComponent
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import (
    AggregatedTransform,
    SparkFunctionTransform,
)


class FeatureSet(HookableComponent):
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

    >>> from butterfree.transform import FeatureSet
    >>> from butterfree.transform.features import (
    ...     Feature,
    ...     KeyFeature,
    ...     TimestampFeature,
    ...)
    >>> from butterfree.transform.transformations import (
    ...     SparkFunctionTransform,
    ...     CustomTransform,
    ... )
    >>> from butterfree.constants import DataType
    >>> from butterfree.transform.utils import Function
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
    ...            transformation=SparkFunctionTransform(
    ...                 functions=[
    ...                            Function(F.avg, DataType.DOUBLE),
    ...                            Function(F.stddev_pop, DataType.DOUBLE)]
    ...             ).with_window(
    ...                 partition_by="id",
    ...                 order_by=TIMESTAMP_COLUMN,
    ...                 mode="fixed_windows",
    ...                 window_definition=["2 minutes", "15 minutes"],
    ...             ),
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
        super().__init__()
        self.name = name
        self.entity = entity
        self.description = description
        self.keys = keys
        self.timestamp = timestamp
        self.features = features
        self.incremental_strategy = IncrementalStrategy(column=TIMESTAMP_COLUMN)

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
    def _get_features_columns(*features: Feature) -> List[str]:
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
    def timestamp(self, value: TimestampFeature) -> None:
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
    def features(self, value: List[Feature]) -> None:
        if not isinstance(value, list) or not all(
            isinstance(item, Feature) for item in value
        ):
            raise ValueError("features needs to be a list of Feature objects.")

        feature_columns = self._get_features_columns(*value)
        if len(feature_columns) != len(set(feature_columns)):
            raise KeyError("feature columns will have duplicates.")

        if self._has_aggregated_transform(value):
            raise ValueError(
                "You can't define a feature within aggregated transform. "
                "You need to use Aggregated Feature Set."
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

    def get_schema(self) -> List[Dict[str, Any]]:
        """Get feature set schema.

        Returns:
            List of dicts regarding cassandra feature set schema.

        """
        schema = []

        for f in self.keys + [self.timestamp]:
            for c in self._get_features_columns(f):
                schema.append(
                    {
                        "column_name": c,
                        "type": f.dtype.spark,
                        "primary_key": True if isinstance(f, KeyFeature) else False,
                    }
                )

        for f in self.features:  # type: ignore
            name = self._get_features_columns(f)
            if isinstance(f.transformation, SparkFunctionTransform):
                type = [
                    fc.data_type.spark
                    for fc in f.transformation.functions
                    for _ in range(len(f.transformation._windows or [None]))
                ]
            else:
                type = [f.dtype.spark]

            for n, dt in zip(name, type):
                schema.append({"column_name": n, "type": dt, "primary_key": False})

        return schema

    @staticmethod
    def _has_aggregated_transform(features: List[Feature]) -> bool:
        """Aggregated Transform mode check.

        Checks if there's a rolling window mode within the scope of the
        AggregatedTransform.

        Returns:
            True if there's a rolling window aggregation mode.

        """
        return any(
            [
                isinstance(feature.transformation, AggregatedTransform)
                for feature in features
            ]
        )

    def _filter_duplicated_rows(self, df: DataFrame) -> DataFrame:
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
        window_key_columns = Window.partitionBy(
            self.keys_columns  # type: ignore
        ).orderBy(TIMESTAMP_COLUMN)
        window_all_columns = Window.partitionBy(
            self.keys_columns + self.features_columns  # type: ignore
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

    def define_start_date(self, start_date: str = None) -> Optional[str]:
        """Get feature set start date.

        Args:
            start_date: start date regarding source dataframe.

        Returns:
            start date.
        """
        return start_date

    def construct(
        self,
        dataframe: DataFrame,
        client: SparkClient,
        end_date: str = None,
        num_processors: int = None,
        start_date: str = None,
    ) -> DataFrame:
        """Use all the features to build the feature set dataframe.

        After that, there's the caching of the dataframe, however since cache()
        in Spark is lazy, an action is triggered in order to force persistence.

        Args:
            dataframe: input dataframe to be transformed by the features.
            client: client responsible for connecting to Spark session.
            start_date: user defined start date.
            end_date: user defined end date.
            num_processors: cluster total number of processors for repartitioning.

        Returns:
            Spark dataframe with all the feature columns.

        """
        if not isinstance(dataframe, DataFrame):
            raise ValueError("source_df must be a dataframe")

        pre_hook_df = self.run_pre_hooks(dataframe)

        output_df = reduce(
            lambda df, feature: feature.transform(df),
            self.keys + [self.timestamp] + self.features,
            pre_hook_df,
        ).select(*self.columns)

        if not output_df.isStreaming:
            output_df = self._filter_duplicated_rows(output_df)
            output_df.cache().count()

        output_df = self.incremental_strategy.filter_with_incremental_strategy(
            dataframe=output_df, start_date=start_date, end_date=end_date
        )

        post_hook_df = self.run_post_hooks(output_df)

        return post_hook_df
