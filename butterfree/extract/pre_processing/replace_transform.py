"""Replace transformer for dataframes."""
from itertools import chain
from typing import Dict

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import coalesce, col, create_map, lit


def replace(
    dataframe: DataFrame, column: str, replace_dict: Dict[str, str]
) -> DataFrame:
    """Replace values of a string column in the dataframe using a dict.

    Example:

    >>> from butterfree.extract.pre_processing import replace
    ... from butterfree.testing.dataframe import (
    ...     assert_dataframe_equality,
    ...     create_df_from_collection,
    ... )
    >>> from pyspark import SparkContext
    >>> from pyspark.sql import session
    >>> spark_context = SparkContext.getOrCreate()
    >>> spark_session = session.SparkSession(spark_context)
    >>> input_data = [
    ...     {"id":1, "type": "a"}, {"id":2, "type": "b"}, {"id":3, "type": "c"}
    ... ]
    >>> input_df = create_df_from_collection(input_data, spark_context, spark_session)
    >>> input_df.collect()

    [Row(id=1, type='a'), Row(id=2, type='b'), Row(id=3, type='c')]

    >>> replace_dict = {"a": "type_a", "b": "type_b"}
    >>> replace(input_df, "type", replace_dict).collect()

    [Row(id=1, type='type_a'), Row(id=2, type='type_b'), Row(id=3, type='c')]

    Args:
        dataframe: data to be transformed.
        column: string column on the dataframe where to apply the replace.
        replace_dict: dict with values to be replaced.
            All mapped values must be string.

    Returns:
        Dataframe with column values replaced.

    """
    if not isinstance(dataframe, DataFrame):
        raise ValueError("dataframe needs to be a Pyspark DataFrame type")
    if (column not in dict(dataframe.dtypes)) or (
        dict(dataframe.dtypes)[column] != "string"
    ):
        raise ValueError("column needs to be the name of an string column in dataframe")
    if (not isinstance(replace_dict, dict)) or (
        not all(isinstance(value, str) for value in chain(*replace_dict.items()))
    ):
        raise ValueError(
            "replace_dict needs to be a Python dict with "
            "all keys and values as string values"
        )

    mapping = create_map(
        [lit(value) for value in chain(*replace_dict.items())]  # type: ignore
    )
    return dataframe.withColumn(column, coalesce(mapping[col(column)], col(column)))
