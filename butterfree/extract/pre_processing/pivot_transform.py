"""Pivot Transform for dataframes."""
from typing import Callable, List, Union

from pyspark.sql import DataFrame, functions
from pyspark.sql.types import DataType

from butterfree.extract.pre_processing import forward_fill


def pivot(
    dataframe: DataFrame,
    group_by_columns: List[str],
    pivot_column: str,
    agg_column: str,
    aggregation: Callable,
    mock_value: Union[float, str] = None,
    mock_type: Union[DataType, str] = None,
    with_forward_fill: bool = False,
) -> DataFrame:
    """Defines a pivot transformation.

    Attributes:
        dataframe: dataframe to be pivoted.
        group_by_columns: list of columns' names to be grouped.
        pivot_column: column to be pivoted.
        agg_column: column to be aggregated by pivoted category.
        aggregation: desired spark aggregation function to be performed.
            An example: spark_agg(col_name). See docs for all spark_agg:
            https://spark.apache.org/docs/2.3.1/api/python/_modules/pyspark/sql/functions.html
        mock_value: value used to make a difference between true nulls resulting from
            the aggregation and empty values from the pivot transformation.
        mock_type: mock_value data type (compatible with spark).
        with_forward_fill: applies a forward fill to null values after the pivot
            operation.

    Example:

        >>> dataframe.orderBy("ts", "id", "amenity").show()
        +---+---+-------+-----+
        | id| ts|amenity|  has|
        +---+---+-------+-----+
        |  1|  1| fridge|false|
        |  1|  1|   oven| true|
        |  1|  1|   pool|false|
        |  2|  2|balcony|false|
        |  1|  3|balcony| null|
        |  1|  4|   oven| null|
        |  1|  4|   pool| true|
        |  1|  5|balcony| true|
        +---+---+-------+-----+

        >>> pivoted = pivot(dataframe, ["id", "ts"], "amenity", "has", functions.first)
        >>> pivoted.orderBy("ts", "id").show()
        +---+---+-------+------+----+-----+
        | id| ts|balcony|fridge|oven| pool|
        +---+---+-------+------+----+-----+
        |  1|  1|   null| false|true|false|
        |  2|  2|  false|  null|null| null|
        |  1|  3|   null|  null|null| null|
        |  1|  4|   null|  null|null| true|
        |  1|  5|   true|  null|null| null|
        +---+---+-------+------+----+-----+

        But, sometimes, you would like to keep the last values that some feature has
        assumed from previous modifications. In this example, amenity "oven" for the
        id=1 was set to null and "pool" was set to true at ts=4. All other amenities
        should then be kept to their actual state at that ts. To do that, we will use
        a technique called forward fill:

        >>> pivoted = pivot(
        ...     dataframe,
        ...     ["id", "ts"],
        ...     "amenity",
        ...     "has",
        ...     functions.first,
        ...     with_forward_fill=True
        ...)
        >>> pivoted.orderBy("ts", "id").show()
        +---+---+-------+------+----+-----+
        | id| ts|balcony|fridge|oven| pool|
        +---+---+-------+------+----+-----+
        |  1|  1|   null| false|true|false|
        |  2|  2|  false|  null|null| null|
        |  1|  3|   null| false|true|false|
        |  1|  4|   null| false|true| true|
        |  1|  5|   true| false|true| true|
        +---+---+-------+------+----+-----+

        Great! Now every amenity that didn't have been changed kept it's state. BUT,
        the force change to null for amenity "oven" on id=1 at ts=4 was ignored during
        forward fill. If the user wants to respect this change, it must provide a mock
        value and type to be used as a signal for "true nulls". In other words, we want
        to forward fill only nulls that were created by the pivot transformation.

        In this example, amenities only assume boolean values. So there is no mock
        values for a boolean. It is only true or false. So users can give a mock value
        of another type (for which the column can be cast to). Check this out:

        >>> pivoted = pivot(
        ...     dataframe,
        ...     ["id", "ts"],
        ...     "amenity",
        ...     "has",
        ...     functions.first,
        ...     with_forward_fill=True,
        ...     mock_value=-1,
        ...     mock_type="int"
        ...)
        >>> pivoted.orderBy("ts", "id").show()
        +---+---+-------+------+----+-----+
        | id| ts|balcony|fridge|oven| pool|
        +---+---+-------+------+----+-----+
        |  1|  1|   null| false|true|false|
        |  2|  2|  false|  null|null| null|
        |  1|  3|   null| false|true|false|
        |  1|  4|   null| false|null| true|
        |  1|  5|   true| false|null| true|
        +---+---+-------+------+----+-----+

        During transformation, this method will cast the agg_column to mock_type
        data type and fill all "true nulls" with the mock_value. After pivot and forward
        fill are applied, all new pivoted columns will then return to the original type
        with all mock values replaced by null.
    """
    agg_column_type = None

    if mock_value is not None:
        if mock_type is None:
            raise AttributeError(
                "When proving a mock value, users must inform the data type,"
                " which should be supported by Spark."
            )
        agg_column_type = dict(dataframe.dtypes).get(agg_column)
        dataframe = dataframe.withColumn(
            agg_column, functions.col(agg_column).cast(mock_type)
        ).fillna({agg_column: mock_value})

    pivoted = (
        dataframe.groupBy(*group_by_columns)
        .pivot(pivot_column)
        .agg(aggregation(agg_column))
    )

    new_columns = [c for c in pivoted.columns if c not in group_by_columns]

    if with_forward_fill:
        for c in new_columns:
            pivoted = forward_fill(
                dataframe=pivoted,
                partition_by=group_by_columns[:-1],
                order_by=group_by_columns[-1],
                fill_column=c,
            )

    if mock_value is not None:
        for c in new_columns:
            pivoted = pivoted.withColumn(
                c,
                functions.when(functions.col(c) != mock_value, functions.col(c)).cast(
                    agg_column_type  # type: ignore
                ),
            )
    return pivoted
