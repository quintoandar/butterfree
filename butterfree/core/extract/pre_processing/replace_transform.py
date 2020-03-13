"""Module where filter DataFrames coming from readers."""
from pyspark.sql.dataframe import DataFrame


def replace(dataframe: DataFrame, column, replace_dict):
    """Replace values of a column in the dataframe using a dict.

    Args:
        dataframe: data to be transformed.
        column: column where to apply the replace.
        replace_dict: mapping of the values to be replaced.

    Returns:
        Dataframe with column values replaced.
    """
    
    return dataframe.filter(condition)
