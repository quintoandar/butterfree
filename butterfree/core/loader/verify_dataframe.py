"""Holds verify dataframe's informations to write in Loader."""
from pyspark.sql.dataframe import DataFrame

from butterfree.core.constant.columns import TIMESTAMP_COLUMN


def verify_column_ts(dataframe: DataFrame):
    """Verify dataframe's columns.

    Args:
        dataframe: spark dataframe containing data from a feature set.

    Returns:
        dataframe: spark dataframe containing data from a feature set.
    """
    if TIMESTAMP_COLUMN not in dataframe.columns:
        raise ValueError("DataFrame must have a 'ts' column.")

    return dataframe
