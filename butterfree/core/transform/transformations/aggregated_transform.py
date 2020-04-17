"""Aggregated Transform entity."""
import itertools
from functools import reduce
from typing import List

from parameters_validation import non_blank
from pyspark.sql import DataFrame, functions
from pyspark.sql.functions import col

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.transformations.transform_component import (
    TransformComponent,
)
from butterfree.core.transform.transformations.user_defined_functions import mode
from butterfree.core.transform.utils import Window


class AggregatedTransform(TransformComponent):
    """Defines an Aggregation.

    Attributes:
        group_by: list of columns to group by.
        functions: aggregation functions, such as avg, std, count.
        column: column to be used in aggregation function.

    Example:
        It's necessary to declare the desired aggregation method, (average,
        standard deviation and count are currently supported, as it can be
        seen in __ALLOWED_AGGREGATIONS), the group by column and aggregation column.
        >>> from butterfree.core.transform.transformations import AggregatedTransform
        >>> from butterfree.core.transform.features import Feature
        >>> from pyspark import SparkContext
        >>> from pyspark.sql import session
        >>> from pyspark.sql.types import TimestampType
        >>> sc = SparkContext.getOrCreate()
        >>> spark = session.SparkSession(sc)
        >>> df = spark.createDataFrame([(1, "2016-04-11 11:31:11", 200),
        ...                             (1, "2016-04-11 11:44:12", 300),
        ...                             (1, "2016-04-11 11:46:24", 400),
        ...                             (1, "2016-04-11 12:03:21", 500)]
        ...                           ).toDF("id", "timestamp", "feature")
        >>> df = df.withColumn("timestamp", df.timestamp.cast(TimestampType()))
        >>> feature = Feature(
        ...    name="feature",
        ...    description="aggregated transform",
        ...    transformation=AggregatedTransform(
        ...        functions=["avg"],
        ...        group_by="id",
        ...        column="feature",
        ...    )
        ...)
        >>> feature.transform(df).show()
        +---+-----------+
        | id|feature_avg|
        +---+-----------+
        |  1|      350.0|
        +---+-----------+

        We can use this transformation with windows.
        >>> feature_rolling_windows = Feature(
        ...    name="feature",
        ...    description="aggregated transform with rolling windows usage example",
        ...    transformation=AggregatedTransform(
        ...        functions=["avg"],
        ...        group_by="id",
        ...        column="feature",
        ...    ).with_window(
        ...        window_definition=["1 day"],
        ...   )
        ...)
        >>> feature_rolling_windows.transform(df).orderBy("timestamp").show()
        +---+-------------------+---------------------------------------+
        | id|          timestamp|feature_avg_over_1_day_rolling_windows|
        +---+-------------------+---------------------------------------+
        |  1|2016-04-11 21:00:00|                                  350.0|
        +---+-------------------+---------------------------------------+

        It's important to notice that transformation affects the
        dataframe granularity and, as it's possible to see, returns only
        columns related to its transformation.
    """

    def __init__(
        self, group_by, functions: non_blank(List[str]), column: non_blank(str),
    ):
        super(AggregatedTransform, self).__init__()
        self.group_by = group_by
        self.functions = functions
        self.column = column
        self._windows = []
        self._pivot_column = None
        self._pivot_values = []
        self._distinct_column = None

    __ALLOWED_AGGREGATIONS = {
        "approx_count_distinct": functions.approx_count_distinct,
        "avg": functions.avg,
        "collect_list": functions.collect_list,
        "collect_set": functions.collect_set,
        "count": functions.count,
        "first": functions.first,
        "kurtosis": functions.kurtosis,
        "last": functions.first,
        "max": functions.max,
        "min": functions.min,
        "mode": mode,
        "skewness": functions.skewness,
        "stddev": functions.stddev,
        "stddev_pop": functions.stddev_pop,
        "sum": functions.sum,
        "sum_distinct": functions.sumDistinct,
        "variance": functions.variance,
        "var_pop": functions.var_pop,
    }

    @property
    def has_windows(self):
        """Aggregated Transform window check.

        Checks the number of windows within the scope of the
        all AggregatedTransform.

        Returns:
            True if the number of windows is greater than zero in all.

        """
        return len(self._windows) > 0

    @property
    def functions(self) -> List[str]:
        """Aggregated functions to be used in the transformation."""
        return self._functions

    @functions.setter
    def functions(self, value: List[str]):
        """Aggregated definitions to be used in the transformation."""
        aggregations = []
        if not value:
            raise ValueError("Aggregations must not be empty.")
        for agg in value:
            if agg not in self.allowed_aggregations:
                raise KeyError(
                    f"{agg} is not supported. These are the allowed "
                    f"aggregations that you can use: "
                    f"{self.allowed_aggregations}"
                )
            aggregations.append(agg)
        self._functions = aggregations

    @property
    def allowed_aggregations(self) -> List[str]:
        """Allowed aggregations to be used in the transformation."""
        return list(self.__ALLOWED_AGGREGATIONS.keys())

    def with_window(self, window_definition):
        """Create a list with windows defined."""
        self._windows = [
            Window(
                partition_by=None,
                order_by=None,
                mode="rolling_windows",
                window_definition=definition,
            )
            for definition in window_definition
        ]
        return self

    def with_pivot(self, pivot_column, pivot_values):
        """This method receives columns to perform the pivot on GroupedData."""
        self._pivot_column = pivot_column
        self._pivot_values = pivot_values
        return self

    def with_distinct(self, distinct_column):
        """This method receives the column where it will perform the distinction."""
        self._distinct_column = distinct_column
        return self

    def _get_output_name(self, function, window=None, pivot_value=None):
        base_name = "__".join([self._parent.name, function])

        if pivot_value:
            base_name = "_".join([pivot_value, base_name])

        if self._windows:
            return "_".join([base_name, window.get_name()])

        return base_name

    def _dataframe_list_join(self, df_base, df):
        if self._windows:
            return df_base.join(
                df, on=[f"{self.group_by}", f"{TIMESTAMP_COLUMN}"], how="full_outer"
            )
        return df_base.join(df, on=self.group_by, how="full_outer")

    @property
    def output_columns(self) -> List[str]:
        """Columns generated by the transformation."""
        output_columns = self._generate_output_columns(
            function=self.functions,
            window=self._windows,
            pivot_value=self._pivot_values,
        )

        if self._windows or self._pivot_column:
            output_columns = list(itertools.chain.from_iterable(output_columns))

        return output_columns

    def _generate_output_columns(self, function, window=None, pivot_value=None):
        if isinstance(function, list):
            return [
                self._generate_output_columns(f, window, pivot_value) for f in function
            ]

        if window and isinstance(window, list):
            return [
                self._generate_output_columns(function, w, pivot_value) for w in window
            ]

        if pivot_value and isinstance(pivot_value, list):
            return [
                self._generate_output_columns(function, window, v) for v in pivot_value
            ]

        output_columns = self._get_output_name(function, window, pivot_value)

        return output_columns

    def aggregate(
        self, dataframe, groupby, aggfunc, aggcolumn, window=None, pivot_on=None
    ):
        """Compute aggregates on Grouped Dataframe."""
        if isinstance(aggfunc, list):
            return [
                self.aggregate(dataframe, groupby, f, aggcolumn, window, pivot_on)
                for f in aggfunc
            ]

        if window and isinstance(window, list):
            return [
                self.aggregate(dataframe, groupby, aggfunc, aggcolumn, w, pivot_on)
                for w in window
            ]

        if self._distinct_column:
            data_on = dataframe.groupBy(
                groupby,
                self._distinct_column,
                window.get() if window else TIMESTAMP_COLUMN,
            ).agg(
                self.__ALLOWED_AGGREGATIONS["max"](TIMESTAMP_COLUMN).alias(
                    TIMESTAMP_COLUMN
                )
            )
            dataframe = (
                data_on.alias("a")
                .join(
                    dataframe.alias("b"),
                    (
                        col(f"a.{self._distinct_column}")
                        == col(f"b.{self._distinct_column}")
                    )
                    & (col(f"a.{TIMESTAMP_COLUMN}") == col(f"b.{TIMESTAMP_COLUMN}")),
                    "left",
                )
                .select("a.*", self.column)
            )

        data = (
            dataframe.groupBy(groupby)
            if not window
            else dataframe.groupBy(
                groupby, "window" if self._distinct_column else window.get()
            )
        )

        if pivot_on:
            data = data.pivot(pivot_on, self._pivot_values)

        df = data.agg(self.__ALLOWED_AGGREGATIONS[aggfunc](aggcolumn))

        return self.with_renamed_columns(
            df, groupby, aggfunc, aggcolumn, window, pivot_on
        )

    def with_renamed_columns(
        self, dataframe, groupby, aggfunc, aggcolumn, window=None, pivot_on=None
    ):
        """Renamed the columns of the dataframe."""
        columns_select = (
            list(itertools.chain.from_iterable([groupby]))
            if isinstance(groupby, list)
            else [groupby]
        )

        if window:
            columns_select.append(functions.col("window.end").alias(TIMESTAMP_COLUMN))

        if pivot_on:
            columns_select += [
                functions.col(column).alias(
                    self._get_output_name(aggfunc, window, column)
                )
                for column in self._pivot_values
            ]
        else:
            columns_select.append(
                functions.col(f"{aggfunc}({aggcolumn})").alias(
                    self._get_output_name(function=aggfunc, window=window)
                )
            )

        return dataframe.select(*columns_select)

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe.

        Returns:
            Transformed dataframe.
        """
        agg_df = self.aggregate(
            dataframe=dataframe,
            groupby=self.group_by,
            aggfunc=self.functions,
            aggcolumn=self.column,
            window=self._windows,
            pivot_on=self._pivot_column,
        )

        if self._windows:
            agg_df = itertools.chain.from_iterable(agg_df)

        return reduce(self._dataframe_list_join, agg_df)
