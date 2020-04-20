"""AggregatedFeatureSet entity."""
import itertools
from datetime import timedelta
from functools import reduce
from typing import Dict, List

from pyspark.sql import DataFrame, functions

from butterfree.core.clients import SparkClient
from butterfree.core.dataframe_service import repartition_sort_df
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import AggregatedTransform
from butterfree.core.transform.utils import Window, date_range


class AggregatedFeatureSet(FeatureSet):
    """Holds metadata about the aggregated feature set.

    This class overrides some methods of the parent FeatureSet class and has specific
    methods for aggregations.

    The AggregatedTransform can only be used on AggregatedFeatureSets. The construct
    method will be responsible by collecting every feature's transformation definitions
    so it can run a groupby over the input dataframe, taking into account whether the
    user want's to run an rolling window aggregation, pivoting or just apply aggregation
    functions.

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
    >>> from butterfree.core.constants.data_type import DataType
    >>> from butterfree.core.clients import SparkClient
    >>> client = SparkClient()
    >>> client.conn.conf.set("spark.sql.session.timeZone", "UTC")
    >>> dataframe = client.conn.createDataFrame(
    ...     [
    ...         (1, "2020-01-01 13:01:00+000", 1000, "publicado"),
    ...         (2, "2020-01-01 14:01:00+000", 2000, "publicado"),
    ...         (1, "2020-01-02 13:01:00+000", 2000, "alugado"),
    ...         (1, "2020-01-03 13:01:00+000", 1000, "despublicado"),
    ...         (2, "2020-01-09 14:01:00+000", 1000, "despublicado"),
    ...     ],
    ...     ("id", "ts", "rent", "status"),
    ... )
    >>> dataframe = dataframe.withColumn("ts", dataframe["ts"].cast("timestamp"))
    >>> feature_set = AggregatedFeatureSet(
    ...    name="aggregated_feature_set",
    ...    entity="entity",
    ...    description="description",
    ...    features=[
    ...        Feature(
    ...            name="feature1",
    ...            description="test",
    ...            transformation=AggregatedTransform(
    ...                 functions=["avg", "stddev_pop"],
    ...             ),
    ...             dtype=DataType.DOUBLE,
    ...             from_column="rent",
    ...        ),
    ...    ],
    ...    keys=[KeyFeature(name="id", description="lul")],
    ...    timestamp=TimestampFeature(from_column="ts"),
    ...)
    >>> result = feature_set.construct(
    ...     dataframe=dataframe,
    ...     client=client,
    ...     end_date="2020-01-15"
    ... )
    >>> result.show()
    +---+-------------------+-------------+----------------+
    | id|          timestamp|feature1__avg|feature1__stddev|
    +---+-------------------+-------------+----------------+
    |  1|2020-01-01 13:01:00|       1000.0|            null|
    |  1|2020-01-03 13:01:00|       1000.0|            null|
    |  1|2020-01-02 13:01:00|       2000.0|            null|
    |  2|2020-01-09 14:01:00|       1000.0|            null|
    |  2|2020-01-01 14:01:00|       2000.0|            null|
    +---+-------------------+-------------+----------------+

    Since you didn't define a window, the AggregateFeatureSet will always group by
    keys and timestamp feature columns. So in this example, there will be no changes to
    the dataframe, since it doesn't duplicate on id and timestamp :)

    Let's run one example with windows:

    >>> feature_set.with_windows(definitions=["3 days"])
    >>> result = feature_set.construct(
    ...     dataframe=dataframe,
    ...     client=client,
    ...     end_date="2020-01-15"
    ... )
    >>> result.orderBy("timestamp", "id").show()
    +---+-------------------+-----------------------------------------+
    | id|          timestamp|feature1__avg_over_3_days_rolling_windows|
    +---+-------------------+-----------------------------------------+
    |  1|2020-01-01 00:00:00|                                     null|
    |  2|2020-01-01 00:00:00|                                     null|
    |  1|2020-01-02 00:00:00|                                   1000.0|
    |  2|2020-01-02 00:00:00|                                   2000.0|
    |  1|2020-01-03 00:00:00|                                   1500.0|
    |  1|2020-01-04 00:00:00|                       1333.3333333333333|
    |  1|2020-01-05 00:00:00|                                   1500.0|
    |  2|2020-01-05 00:00:00|                                     null|
    |  1|2020-01-06 00:00:00|                                   1000.0|
    |  1|2020-01-07 00:00:00|                                     null|
    |  2|2020-01-10 00:00:00|                                   1000.0|
    |  2|2020-01-13 00:00:00|                                     null|
    +---+-------------------+-----------------------------------------+

    +---+-------------------+--------------------------------------------+
    | id|          timestamp|feature1__stddev_over_3_days_rolling_windows|
    +---+-------------------+--------------------------------------------+
    |  1|2020-01-01 00:00:00|                                        null|
    |  2|2020-01-01 00:00:00|                                        null|
    |  1|2020-01-02 00:00:00|                                        null|
    |  2|2020-01-02 00:00:00|                                        null|
    |  1|2020-01-03 00:00:00|                           707.1067811865476|
    |  1|2020-01-04 00:00:00|                           577.3502691896258|
    |  1|2020-01-05 00:00:00|                           707.1067811865476|
    |  2|2020-01-05 00:00:00|                                        null|
    |  1|2020-01-06 00:00:00|                                        null|
    |  1|2020-01-07 00:00:00|                                        null|
    |  2|2020-01-10 00:00:00|                                        null|
    |  2|2020-01-13 00:00:00|                                        null|
    +---+-------------------+--------------------------------------------+

    (Had to break down the table result.)

    And with pivot:

    >>> feature_set.with_pivot(column="status", values=["publicado", "despublicado"])
    +---+-------------------+-----------------------+--------------------------+
    | id|          timestamp|publicado_feature1__avg|publicado_feature1__stddev|
    +---+-------------------+-----------------------+--------------------------+
    |  1|2020-01-01 13:01:00|                 1000.0|                      null|
    |  2|2020-01-01 14:01:00|                 2000.0|                      null|
    |  1|2020-01-02 13:01:00|                   null|                      null|
    |  1|2020-01-03 13:01:00|                   null|                      null|
    |  2|2020-01-09 14:01:00|                   null|                      null|
    +---+-------------------+-----------------------+--------------------------+

    +---+-------------------+--------------------------+-----------------------------+
    | id|          timestamp|despublicado_feature1__avg|despublicado_feature1__stddev|
    +---+-------------------+--------------------------+-----------------------------+
    |  1|2020-01-01 13:01:00|                      null|                         null|
    |  2|2020-01-01 14:01:00|                      null|                         null|
    |  1|2020-01-02 13:01:00|                      null|                         null|
    |  1|2020-01-03 13:01:00|                    1000.0|                         null|
    |  2|2020-01-09 14:01:00|                    1000.0|                         null|
    +---+-------------------+--------------------------+-----------------------------+

    As you can see, we need to pass the values you want to pivot. It optimizes this
    processing in spark and allows ignoring values when pivoting. If you wanted to get
    the pivot aggregation for "alugado" too, just use:

    >>> feature_set.with_pivot(
    ...     column="status", values=["publicado", "despublicado", "alugado"]
    ... )

    You can also run it with pivot AND windows:

    >>> feature_set.with_pivot(
    ...     column="status",
    ...     values=["publicado", "despublicado", "alugado"]
    ... ).with_windows(definitions=["1 day", "2 weeks"])

    The construct method will execute the feature set, computing all the
    defined aggregated transformations at once.

    Remember: when using an AggregatedFeatureSet without window, the group will use the
    timestamp column.
    """

    def __init__(
        self,
        name: str,
        entity: str,
        description: str,
        keys: List[KeyFeature],
        timestamp: TimestampFeature,
        features: List[Feature],
    ):
        self._windows = []
        self._pivot_column = None
        self._pivot_values = []
        super(AggregatedFeatureSet, self).__init__(
            name, entity, description, keys, timestamp, features,
        )

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
    def _build_feature_column_name(feature_column, pivot_value=None, window=None):
        base_name = feature_column
        if pivot_value is not None:
            base_name = f"{pivot_value}_{base_name}"
        if window is not None:
            base_name = f"{base_name}_{window.get_name()}"

        return base_name

    @property
    def features_columns(self) -> List[str]:
        """Name of the columns of all features in feature set."""
        base_columns = list(
            itertools.chain(*[f.get_output_columns() for f in self.features])
        )

        pivot_values = self._pivot_values or [None]
        windows = self._windows or [None]
        feature_columns = [
            self._build_feature_column_name(fc, pivot_value=pv, window=w)
            for pv, fc, w in itertools.product(pivot_values, base_columns, windows)
        ]
        return feature_columns

    def with_windows(self, definitions: List[str]):
        """Create a list with windows defined."""
        self._windows = [
            Window(
                partition_by=None,
                order_by=None,
                mode="rolling_windows",
                window_definition=definition,
            )
            for definition in definitions
        ]
        return self

    def with_pivot(self, column: str, values: List[str]):
        """Add a pivot configuration for your aggregated feature set.

        This means we will group the input data, pivot over the column parameter and
        run each aggregation function over the columns within each value group in
        the values parameter. In spark it will be something like:
        dataframe.groupBy(*group).pivot(column, values).agg(*aggregations)

        Args:
            column: the column, containing categorical values, to pivot on.
            values: the distinct values you want to be pivoted.

        Returns:
            An AggregatedFeatureSet configured with pivot.
        """
        self._pivot_column = column
        self._pivot_values = values
        return self

    def _get_base_dataframe(self, client, dataframe, end_date):
        start_date = dataframe.agg(functions.min(self.timestamp_column)).take(1)[0][0]
        end_date = end_date or dataframe.agg(functions.max(self.timestamp_column)).take(
            1
        )[0][0] + timedelta(days=1)
        date_df = date_range.get_date_range(client, start_date, end_date)
        unique_keys = dataframe.dropDuplicates(subset=self.keys_columns).select(
            *self.keys_columns
        )

        return unique_keys.crossJoin(date_df)

    @staticmethod
    def _dataframe_join(left, right, on, how):
        return left.join(right, on=on, how=how)

    def _aggregate(self, dataframe, features, window=None):
        aggregations = list(
            itertools.chain.from_iterable(
                [f.transformation.aggregations for f in features]
            )
        )
        grouped_data = (
            dataframe.groupby(*self.keys_columns, window.get())
            if window is not None
            else dataframe.groupby(*self.keys_columns, self.timestamp_column)
        )

        if self._pivot_column:
            grouped_data = grouped_data.pivot(self._pivot_column, self._pivot_values)

        aggregated = grouped_data.agg(*aggregations)
        return self._with_renamed_columns(aggregated, features, window)

    def _with_renamed_columns(self, aggregated, features, window):
        old_columns = [
            c
            for c in aggregated.columns
            if c not in self.keys_columns + [self.timestamp_column, "window"]
        ]

        # the loop order here is really important
        # spark will always create the aggregated dataframe following the order:
        # pivot_value then aggregation function
        # so when we have 2 pivot values and 2 agg functions
        # the result columns will be something like: pivot_value1_function1,
        # pivot_value1_function_2, pivot_value2_function1 and pivot_value2_function2
        base_columns = list(
            itertools.chain(*[f.get_output_columns() for f in features])
        )
        pivot_values = self._pivot_values or [None]
        new_columns = [
            self._build_feature_column_name(fc, pivot_value=pv, window=window)
            for pv, fc in itertools.product(pivot_values, base_columns)
        ]

        select = [
            f"`{old_name}` as {new_name}"
            for old_name, new_name in zip(old_columns, new_columns)
        ]
        if self._windows:
            select.append(f"window.end as {self.timestamp_column}")
        else:
            select.append(f"{self.timestamp_column}")

        select += [kc for kc in self.keys_columns]
        return aggregated.selectExpr(*select)

    def get_schema(self) -> List[Dict]:
        """Get feature set schema.

        Returns:
            List of dicts with the feature set schema.

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
        pivot_values = self._pivot_values or [None]
        windows = self._windows or [None]
        for f in self.features:
            combination = itertools.product(
                pivot_values, self._get_features_columns(f), windows
            )
            for pv, fc, w in combination:
                name = self._build_feature_column_name(fc, pivot_value=pv, window=w)

                schema.append(
                    {"column_name": name, "type": f.dtype.spark, "primary_key": False}
                )

        return schema

    def construct(
        self,
        dataframe: DataFrame,
        client: SparkClient,
        end_date: str = None,
        num_processors: int = None,
    ) -> DataFrame:
        """Use all the features to build the feature set dataframe.

        After that, there's the caching of the dataframe, however since cache()
        in Spark is lazy, an action is triggered in order to force persistence,
        but we only cache if it is not a streaming spark dataframe.

        Args:
            dataframe: input dataframe to be transformed by the features.
            client: client responsible for connecting to Spark session.
            end_date: user defined max date for having aggregated data (exclusive).
            num_processors: cluster total number of processors for repartitioning.

        Returns:
            Spark dataframe with all the feature columns.

        """
        if end_date is None and self._windows:
            raise ValueError(
                "When using aggregate with windows, one must give end_date."
            )

        if not isinstance(dataframe, DataFrame):
            raise ValueError("source_df must be a dataframe")

        output_df = reduce(
            lambda df, feature: feature.transform(df),
            self.keys + [self.timestamp],
            dataframe,
        )

        # repartition to have all ids at the same partition
        # by doing that, we won't have to shuffle data on grouping by id
        output_df = repartition_sort_df(
            output_df,
            partition_by=self.keys_columns,
            order_by=[self.timestamp_column],
            num_processors=num_processors,
        )

        if self._windows:
            # prepare our left table, a cartesian product between distinct keys
            # and dates in range for this feature set
            # make the left table co-partitioned with the aggregations' result
            # improving our upcoming joins
            base_df = self._get_base_dataframe(
                client=client, dataframe=output_df, end_date=end_date
            )
            base_df = repartition_sort_df(
                base_df,
                partition_by=self.keys_columns,
                order_by=[self.timestamp_column],
                num_processors=num_processors,
            )

            # run aggregations for each window
            agg_list = [
                self._aggregate(dataframe=output_df, features=self.features, window=w)
                for w in self._windows
            ]

            # left join each aggregation result to our base dataframe
            output_df = reduce(
                lambda left, right: self._dataframe_join(
                    left,
                    right,
                    on=self.keys_columns + [self.timestamp_column],
                    how="left",
                ),
                agg_list,
                base_df,
            )
        else:
            output_df = self._aggregate(output_df, features=self.features)

        output_df = (
            repartition_sort_df(
                output_df,
                partition_by=self.keys_columns,
                order_by=[self.timestamp_column],
                num_processors=num_processors,
            )
            .select(*self.columns)
            .replace(float("nan"), None)
        )

        if not output_df.isStreaming:
            output_df = self._filter_duplicated_rows(output_df)
            output_df.cache().count()

        return output_df
