"""Aggregated Transform entity."""
import itertools
from functools import reduce
from typing import List

from parameters_validation import non_blank
from pyspark.sql import DataFrame, functions

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.transformations.transform_component import (
    TransformComponent,
)
from butterfree.core.transform.transformations.user_defined_functions import mode
from butterfree.core.transform.utils import Window


class AggregatedTransform(TransformComponent):
    """Specifies an aggregation.

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

    def __init__(self, functions: non_blank(List[str])):
        super(AggregatedTransform, self).__init__()
        self.functions = functions

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
    def aggregations(self):
        return [
            self.__ALLOWED_AGGREGATIONS[f](
                self._parent.from_column or self._parent.name
            )
            for f in self.functions
        ]

    @property
    def allowed_aggregations(self) -> List[str]:
        """Allowed aggregations to be used in the transformation."""
        return list(self.__ALLOWED_AGGREGATIONS.keys())

    def _get_output_name(self, function):
        return "__".join([self._parent.name, function])

    @property
    def output_columns(self) -> List[str]:
        return [self._get_output_name(f) for f in self.functions]

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe.

        Returns:
            Transformed dataframe.
        """
        raise NotImplementedError(
            "AggregatedTransform won't be used outside an AggregatedFeatureSet, "
            "meaning the responsibility of aggregating and apply the transformation is "
            "now over the FeatureSet component. This should optimize the ETL process."
        )
