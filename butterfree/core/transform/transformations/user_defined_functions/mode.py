"""Method to compute mode aggregation."""
from pyspark.sql.functions import PandasUDFType, pandas_udf
from pyspark.sql.types import StringType


@pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
def mode(column):
    """Computes a mode aggregation.

    Attributes:
        column: desired data to be aggregated with mode.

    Example:
        It's necessary to declare the desired aggregation method, (average,
        standard deviation and count are currently supported, as it can be
        seen in __ALLOWED_AGGREGATIONS), the partition column, choose both
        window lenght and time unit and, finally, define the mode.
        >>> from pyspark import SparkContext
        >>> from pyspark.sql import session, Window
        >>> from pyspark.sql.functions import PandasUDFType, pandas_udf
        >>> from pyspark.sql.types import StringType
        >>> sc = SparkContext.getOrCreate()
        >>> spark = session.SparkSession(sc)
        >>> df = spark.createDataFrame(
        >>>      [(1, 1), (1, 1), (2, 2), (2, 1), (2, 2)],
        >>>      ("id", "column"))
        >>> @pandas_udf(StringType(), PandasUDFType.GROUPED_AGG)
        ... def mode(column):
        ...    return str(column.value_counts().index.tolist()[:10])
        >>> df.groupby("id").agg(mode("column")).show()
        +---+------------+
        | id|mode(column)|
        +---+------------+
        |  1|         [1]|
        |  2|      [2, 1]|
        +---+------------+
        >>> w = Window \
        ...     .partitionBy('id') \
        ...     .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        >>> df.withColumn('most_viewed', mode("column").over(w)).show()
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
        frequent values (limited to ten). It's important to notice, however, that
        if we want to used in fixed_windows or row_windows mode, we'd need unbounded
        windows. For that reason, mode is meant to be used just in rolling_windows
        mode, initially. We intend to make it available to others modes soon.

    """
    return str(column.value_counts().index.tolist()[:10])
