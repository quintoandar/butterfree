"""Method to compute mode aggregation."""
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType


@pandas_udf(StringType())  # type: ignore
def mode(column: pd.Series) -> str:
    """Computes a mode aggregation.

    Attributes:
        column: desired data to be aggregated with mode.

    Example:
        It's necessary to declare the desired aggregation method, (average,
        standard deviation and count are currently supported, as it can be
        seen in __ALLOWED_AGGREGATIONS) and, finally, define the mode.

        >>> from pyspark import SparkContext
        >>> from pyspark.sql import session, Window
        >>> from pyspark.sql.functions import pandas_udf
        >>> from butterfree.transform\
        ...      .transformations.user_defined_functions import (mode)
        >>> sc = SparkContext.getOrCreate()
        >>> spark = session.SparkSession(sc)
        >>> df = spark.createDataFrame(
        >>>      [(1, 1), (1, 1), (2, 2), (2, 1), (2, 2)],
        >>>      ("id", "column"))
        >>> df.groupby("id").agg(mode("column")).show()
        +---+------------+
        | id|mode(column)|
        +---+------------+
        |  1|           1|
        |  2|           2|
        +---+------------+
        >>> w = Window.partitionBy('id').rowsBetween(
        ...       Window.unboundedPreceding, Window.unboundedFollowing)
        >>> df.withColumn('most_viewed', mode("column").over(w)).show()
        +---+------+-----------+
        | id|column|most_viewed|
        +---+------+-----------+
        |  1|     1|          1|
        |  1|     1|          1|
        |  2|     2|          2|
        |  2|     1|          2|
        |  2|     2|          2|
        +---+------+-----------+

        This example shows the mode aggregation. It's important to notice,
        however, that if we want to used in fixed_windows or row_windows mode,
        we'd need unbounded windows. For that reason, mode is meant to be used
        just in rolling_windows mode, initially. We intend to make it available
        to others modes soon.

    """
    return str(column.mode()[0])
