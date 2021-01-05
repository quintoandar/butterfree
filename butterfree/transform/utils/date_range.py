"""Utils for date range generation."""

from datetime import datetime
from typing import Union

from pyspark.sql import DataFrame, functions

from butterfree.clients import SparkClient
from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN


def get_date_range(
    client: SparkClient,
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    step: int = None,
) -> DataFrame:
    """Create a date range dataframe.

    The dataframe returning from this method will containing a single column
    TIMESTAMP_COLUMN, of timestamp type, with dates between start and end.

    Args:
        client: a spark client.
        start_date: range beginning value (inclusive).
        end_date: range last value (exclusive)
        step: optional step, in seconds.

    Returns:
        A single column date range spark dataframe.
    """
    day_in_seconds = 60 * 60 * 24
    step = step or day_in_seconds
    start_date = (
        start_date if isinstance(start_date, str) else start_date.strftime("%Y-%m-%d")
    )
    end_date = end_date if isinstance(end_date, str) else end_date.strftime("%Y-%m-%d")
    date_df = client.conn.createDataFrame(
        [(start_date, end_date)], ("start_date", "end_date")
    ).select(
        [
            functions.col(c).cast(DataType.TIMESTAMP.spark).cast(DataType.BIGINT.spark)
            for c in ("start_date", "end_date")
        ]
    )
    start_date, end_date = date_df.first()
    return client.conn.range(
        start_date, end_date + day_in_seconds, step  # type: ignore
    ).select(functions.col("id").cast(DataType.TIMESTAMP.spark).alias(TIMESTAMP_COLUMN))
