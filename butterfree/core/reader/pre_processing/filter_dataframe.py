"""Module where filter DataFrames coming from readers."""
from pyspark.sql.dataframe import DataFrame


def filter_dataframe(dataframe: DataFrame, conditions):
    if isinstance(conditions, str):
        return dataframe.filter(conditions)
    elif conditions in dataframe.columns:
        return dataframe.filter(conditions)
    else:
        raise TypeError("condition should be string or DataFrame's column")

