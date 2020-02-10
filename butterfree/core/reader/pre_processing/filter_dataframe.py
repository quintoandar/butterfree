"""Module where filter DataFrames coming from readers."""
from pyspark.sql.dataframe import DataFrame


def filter_dataframe(dataframe: DataFrame, condition):
    """Filters DataFrame's rows using the given condition and value.

    Args:
        dataframe: Spark DataFrame.
        condition: the string SQL expression with column, operation and value.

    Returns:
        DataFrame
    """
    if not isinstance(condition, str):
        raise TypeError("condition should be string.")

    return dataframe.filter(condition)
