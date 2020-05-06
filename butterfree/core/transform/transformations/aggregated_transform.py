"""Aggregated Transform entity."""
from typing import List

from parameters_validation import non_blank
from pyspark.sql import Column, DataFrame, functions
from pyspark.sql.functions import col, expr, when

from butterfree.core.transform.transformations.transform_component import (
    TransformComponent,
)
from butterfree.core.transform.transformations.user_defined_functions import mode
from butterfree.core.transform.transformations.user_defined_functions import most_frequent_set


class AggregatedTransform(TransformComponent):
    """Specifies an aggregation.

    This transformation needs to be used within an AggregatedFeatureSet. Unlike the
    other transformations, this class won't have a transform method implemented.

    The idea behing aggregating is that, in spark, we should execute all aggregation
    functions after a single groupby. So an AggregateFeatureSet will have many Features
    with AggregatedTransform. If each one of them needs to apply a groupby.agg(), then
    we must join all the results in the end, making this computation extremely slow.

    Now, the AggregateFeatureSet will collect all Features' AggregatedTransform
    definitions and run, at once, a groupby.agg(*aggregations).

    This class helps defining on a feature, which aggregation function will be applied
    to build a new aggregated column. Allowed aggregations are registered under the
     allowed_aggregations property.

    Attributes:
        functions: aggregation functions, such as avg, std, count.
        filter_expression: sql boolean expression to be used inside agg function.
            The filter expression can be used to aggregate some column only with
            records that obey certain condition. Has the same behaviour of the
            following SQL expression: `agg(case when filter_expression then col end)`

    Example:
        >>> from butterfree.core.transform.transformations import AggregatedTransform
        >>> from butterfree.core.transform.features import Feature
        >>> from butterfree.core.constants.data_type import DataType
        >>> feature = Feature(
        ...     name="feature",
        ...     description="aggregated transform",
        ...     transformation=AggregatedTransform(
        ...         functions=["avg", "stddev_pop"],
        ...     ),
        ...     dtype=DataType.DOUBLE,
        ...     from_column="somenumber",
        ...)
        >>> feature.get_output_columns()
        ['feature__avg', 'feature__stddev_pop']
        >>> feature.transform(anydf)
        NotImplementedError: ...
    """

    def __init__(self, functions: non_blank(List[str]), filter_expression: str = None):
        super(AggregatedTransform, self).__init__()
        self.functions = functions
        self.filter_expression = filter_expression

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
        "most_frequent_set": most_frequent_set,
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
    def aggregations(self) -> List[Column]:
        """Aggregated spark columns."""
        column_name = self._parent.from_column or self._parent.name

        # if transform has a filter expression apply inside agg function
        # if not, use just the target column name inside agg function
        expression = (
            when(expr(self.filter_expression), col(column_name))
            if self.filter_expression
            else column_name
        )
        return [self.__ALLOWED_AGGREGATIONS[f](expression) for f in self.functions]

    @property
    def allowed_aggregations(self) -> List[str]:
        """Allowed aggregations to be used in the transformation."""
        return list(self.__ALLOWED_AGGREGATIONS.keys())

    def _get_output_name(self, function):
        return "__".join([self._parent.name, function])

    @property
    def output_columns(self) -> List[str]:
        """Columns names generated by the transformation."""
        return [self._get_output_name(f) for f in self.functions]

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """(NotImplemented) Performs a transformation to the feature pipeline.

        For the AggregatedTransform, the transformation won't be applied without
        using an AggregatedFeatureSet.

        Args:
            dataframe: input dataframe.

        Raises:
            NotImplementedError.
        """
        raise NotImplementedError(
            "AggregatedTransform won't be used outside an AggregatedFeatureSet, "
            "meaning the responsibility of aggregating and apply the transformation is "
            "now over the FeatureSet component. This should optimize the ETL process."
        )
