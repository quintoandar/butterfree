"""Forward Fill Transform for dataframes."""
import sys
from typing import List, Union

from pyspark.sql import DataFrame, Window, functions


def forward_fill(
    dataframe: DataFrame,
    partition_by: Union[str, List[str]],
    order_by: Union[str, List[str]],
    fill_column: str,
    filled_column: str = None,
) -> DataFrame:
    """Applies a forward fill to a single column.

    Filling null values with the last known non-null value, leaving leading nulls alone.

    Attributes:
        dataframe: dataframe to be transformed.
        partition_by: list of columns' names to be used as partition for the operation.
        order_by: list of columns' names to be used when sorting column values.
        fill_column: column to be forward filled.
        filled_column: new column name. Optional. When none, operation will be inplace.

    Example:

        >>> dataframe.orderBy("ts", "sensor_type", "location").show()
        +-----------+-------------------+--------+-----------+
        |sensor_type|                 ts|location|temperature|
        +-----------+-------------------+--------+-----------+
        |          1|2017-09-09 12:00:00|   shade|   18.83018|
        |          1|2017-09-09 12:00:00|     sun|       null|
        |          2|2017-09-09 12:00:00|   shade|   18.61258|
        |          2|2017-09-09 12:00:00|     sun|    25.4986|
        |          1|2017-09-09 13:00:00|   shade|   18.78458|
        |          1|2017-09-09 13:00:00|     sun|   25.68457|
        |          2|2017-09-09 13:00:00|   shade|       null|
        |          2|2017-09-09 13:00:00|     sun|       null|
        |          1|2017-09-09 14:00:00|   shade|   17.98115|
        |          1|2017-09-09 14:00:00|     sun|   24.15754|
        |          2|2017-09-09 14:00:00|   shade|   18.61258|
        |          2|2017-09-09 14:00:00|     sun|       null|
        +-----------+-------------------+--------+-----------+

        >>> filled_df = forward_fill(
        ...     dataframe,
        ...     ["sensor_type", "location"],
        ...     "ts",
        ...     "temperature",
        ...     "temperature_filled"
        ... )
        >>> filled_df.orderBy("ts", "sensor_type", "location").show()
        +-----------+-------------------+--------+-----------+------------------+
        |sensor_type|                 ts|location|temperature|temperature_filled|
        +-----------+-------------------+--------+-----------+------------------+
        |          1|2017-09-09 12:00:00|   shade|   18.83018|          18.83018|
        |          1|2017-09-09 12:00:00|     sun|       null|              null|
        |          2|2017-09-09 12:00:00|   shade|   18.61258|          18.61258|
        |          2|2017-09-09 12:00:00|     sun|    25.4986|           25.4986|
        |          1|2017-09-09 13:00:00|   shade|   18.78458|          18.78458|
        |          1|2017-09-09 13:00:00|     sun|   25.68457|          25.68457|
        |          2|2017-09-09 13:00:00|   shade|       null|          18.61258|
        |          2|2017-09-09 13:00:00|     sun|       null|           25.4986|
        |          1|2017-09-09 14:00:00|   shade|   17.98115|          17.98115|
        |          1|2017-09-09 14:00:00|     sun|   24.15754|          24.15754|
        |          2|2017-09-09 14:00:00|   shade|   18.61258|          18.61258|
        |          2|2017-09-09 14:00:00|     sun|       null|           25.4986|
        +-----------+-------------------+--------+-----------+------------------+
        >>> # inplace forward fill
        >>> filled_df = forward_fill(
        ...     dataframe,
        ...     ["sensor_type", "location"],
        ...     "ts",
        ...     "temperature"
        ... )
        >>> filled_df.orderBy("ts", "sensor_type", "location").show()
        +-----------+-------------------+--------+-----------+
        |sensor_type|                 ts|location|temperature|
        +-----------+-------------------+--------+-----------+
        |          1|2017-09-09 12:00:00|   shade|   18.83018|
        |          1|2017-09-09 12:00:00|     sun|       null|
        |          2|2017-09-09 12:00:00|   shade|   18.61258|
        |          2|2017-09-09 12:00:00|     sun|    25.4986|
        |          1|2017-09-09 13:00:00|   shade|   18.78458|
        |          1|2017-09-09 13:00:00|     sun|   25.68457|
        |          2|2017-09-09 13:00:00|   shade|   18.61258|
        |          2|2017-09-09 13:00:00|     sun|    25.4986|
        |          1|2017-09-09 14:00:00|   shade|   17.98115|
        |          1|2017-09-09 14:00:00|     sun|   24.15754|
        |          2|2017-09-09 14:00:00|   shade|   18.61258|
        |          2|2017-09-09 14:00:00|     sun|    25.4986|
        +-----------+-------------------+--------+-----------+
    """
    window = (
        Window.partitionBy(partition_by)  # type: ignore
        .orderBy(order_by)  # type: ignore
        .rowsBetween(-sys.maxsize, 0)
    )

    return dataframe.withColumn(
        filled_column or fill_column,
        functions.last(dataframe[fill_column], ignorenulls=True).over(window),
    )
