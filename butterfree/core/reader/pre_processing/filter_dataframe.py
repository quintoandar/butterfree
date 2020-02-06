"""Module where filter DataFrames coming from readers."""
from pyspark.sql.dataframe import DataFrame


def filter_dataframe(dataframe: DataFrame, column, condition, value):
    """Filters DataFrame's rows using the given condition and value.

    Args:
        dataframe: Spark DataFrame.
        column: DataFrame column.
        condition: the filter condition, example: =, >, <, not in.
        value: the value should be filtered.

    Returns:
        DataFrame
    """
    if not isinstance(column, str):
        raise TypeError("column should be string.")

    if not isinstance(condition, str):
        raise TypeError("condition should be string.")

    if column not in dataframe.columns:
        raise ValueError("column should be a DataFrame's column")

    if condition == "not in":
        return dataframe.filter(~dataframe[column].isin(value))
    else:
        return dataframe.filter("{}" "{}" "{}".format(column, condition, value))
