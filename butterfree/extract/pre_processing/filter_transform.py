"""Module where filter DataFrames coming from readers."""
from pyspark.sql.dataframe import DataFrame


def filter(dataframe: DataFrame, condition: str) -> DataFrame:
    """Filters DataFrame's rows using the given condition and value.

    Args:
        dataframe: Spark DataFrame.
        condition: SQL expression with column, operation and value
            to filter the dataframe.

    Returns:
        Filtered dataframe
    """
    if not isinstance(condition, str):
        raise TypeError("condition should be string.")

    return dataframe.filter(condition)
