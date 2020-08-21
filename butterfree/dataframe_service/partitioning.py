"""Module defining partitioning methods."""

from typing import Any, Dict, List

from pyspark.sql import DataFrame


def extract_partition_values(
    dataframe: DataFrame, patition_columns: List[str]
) -> List[Dict[str, Any]]:
    """Extract distinct partition values from a given dataframe.

    Args:
        dataframe: dataframe from where to extract partition values.
        patition_columns: name of partition columns presented on the dataframe.

    Returns:
        distinct partition values.

    """
    return (
        dataframe.select(*patition_columns)
        .distinct()
        .rdd.map(lambda row: row.asDict(True))
        .collect()
    )
