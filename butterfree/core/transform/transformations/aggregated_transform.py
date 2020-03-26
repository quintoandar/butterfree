"""Aggregated Transform entity."""
from functools import reduce
from typing import List

from parameters_validation import non_blank
from pyspark.sql import DataFrame, functions

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.transformations.transform_component import (
    TransformComponent,
)
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

    def __init__(self, group_by, functions: non_blank(List[str]), column):
        super(AggregatedTransform, self).__init__()
        self.group_by = group_by
        self.functions = functions
        self.column = column
        self._windows = []

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
        "skewness": functions.skewness,
        "stddev": functions.stddev,
        "stddev_pop": functions.stddev_pop,
        "sum": functions.sum,
        "sum_distinct": functions.sumDistinct,
        "variance": functions.variance,
        "var_pop": functions.var_pop,
    }

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

    def _get_output_name(self, function, window=None):
        base_name = "__".join([self._parent.name, function])

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
        output_columns = []
        for function in self.functions:
            if self._windows:
                for window in self._windows:
                    output_columns.append(self._get_output_name(function, window))
            else:
                output_columns.append("_".join([self._parent.name, function]))

        return output_columns

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe.

        Returns:
            Transformed dataframe.
        """
        df_list = []
        for function in self.functions:
            if self._windows:
                for window in self._windows:
                    df_window = (
                        dataframe.groupBy(self.group_by, window.get())
                        .agg(self.__ALLOWED_AGGREGATIONS[function](self.column))
                        .select(
                            self.group_by,
                            functions.col(f"{function}({self.column})").alias(
                                self._get_output_name(function, window)
                            ),
                            functions.col("window.end").alias(TIMESTAMP_COLUMN),
                        )
                    )
                    df_list.append(df_window)
            else:
                df = (
                    dataframe.groupBy(self.group_by)
                    .agg(self.__ALLOWED_AGGREGATIONS[function](self.column))
                    .withColumnRenamed(
                        f"{function}({self.column})", self._get_output_name(function),
                    )
                )
                df_list.append(df)

        if df_list:
            dataframe = reduce(self._dataframe_list_join, df_list)

        return dataframe
