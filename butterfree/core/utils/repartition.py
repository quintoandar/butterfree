"""Module where there are repartition methods."""
from typing import List

from pyspark.sql.dataframe import DataFrame


def repartition_df(dataframe: DataFrame, num_partitions: int, partition_by: List[str]):
    """Partition the DataFrame.

    Args:
        dataframe: Spark DataFrame.
        num_partitions: number of partitions.
        partition_by: list of partitions.

    Returns:
        Partitioned dataframe.

    """
    return dataframe.repartition(num_partitions, *partition_by)


def repartition_sort_df(
    dataframe: DataFrame, num_partitions: int, partition_by: List[str]
):
    """Partition and Sort the DataFrame.

    Args:
        dataframe: Spark DataFrame.
        num_partitions: number of partitions.
        partition_by: list of partitions.

    Returns:
        Partitioned and sorted dataframe.

    """
    dataframe = repartition_df(dataframe, num_partitions, partition_by)
    return dataframe.sortWithinPartitions(*partition_by)
