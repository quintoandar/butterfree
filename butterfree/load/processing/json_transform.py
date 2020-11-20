"""Json conversion for writers."""
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import struct, to_json


def json_transform(dataframe: DataFrame):
    """Filters DataFrame's rows using the given condition and value.

    Args:
        dataframe: Spark DataFrame.

    Returns:
        Converted dataframe.
    """
    return dataframe.select(
        to_json(struct([dataframe[column] for column in dataframe.columns])).alias(
            "value"
        )
    )
