"""Method to compute most frequent aggregation."""
from pyspark.sql.functions import PandasUDFType, pandas_udf
from pyspark.sql.types import ArrayType, LongType


@pandas_udf(ArrayType(LongType()), PandasUDFType.GROUPED_AGG)
def most_frequent_elements_list(column):
    """Computes the most frequent aggregation.

    Attributes:
        column: desired data to be aggregated with most frequent aggregation.

    Example:
        It's necessary to declare the desired aggregation method, (average,
        standard deviation and count are currently supported, as it can be
        seen in __ALLOWED_AGGREGATIONS) and define the most frequent aggregation.
        >>> from pyspark import SparkContext
        >>> from pyspark.sql import session, Window
        >>> from pyspark.sql.functions import PandasUDFType, pandas_udf
        >>> from pyspark.sql.types import ArrayType, LongType
        >>> sc = SparkContext.getOrCreate()
        >>> spark = session.SparkSession(sc)
        >>> df = spark.createDataFrame(
        >>>      [(1, 1), (1, 1), (2, 2), (2, 1), (2, 2)],
        >>>      ("id", "column"))
        >>> @pandas_udf(ArrayType(LongType()), PandasUDFType.GROUPED_AGG)
        ... def most_frequent_elements_list(column):
        ...    return column.value_counts().index.tolist()
        >>> df.groupby("id").agg(most_frequent_elements_list("column")).show()
        +---+-----------------------------------+
        | id|most_frequent_elements_list(column)|
        +---+-----------------------------------+
        |  1|                                [1]|
        |  2|                             [2, 1]|
        +---+-----------------------------------+
        >>> w = Window \
        ...     .partitionBy('id') \
        ...     .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        >>> df.withColumn(
        ...     'most_viewed', most_frequent_elements_list("column").over(w)
        ... ).show()
        +---+------+-----------+
        | id|column|most_viewed|
        +---+------+-----------+
        |  1|     1|        [1]|
        |  1|     1|        [1]|
        |  2|     2|     [2, 1]|
        |  2|     1|     [2, 1]|
        |  2|     2|     [2, 1]|
        +---+------+-----------+
        This example shows the mode aggregation. It returns a list with the most
        frequent values. It's important to notice, however, that if we want to
        use it in fixed_windows or row_windows mode, we'd need unbounded windows.
        For that reason, mode is meant to be used just in rolling_windows mode,
        initially. We intend to make it available to others modes soon.
    """
    return column.value_counts().index.tolist()
