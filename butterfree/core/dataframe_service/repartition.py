"""Module where there are repartition methods."""
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.core.constants.spark_constants import (
    DEFAULT_NUM_PARTITIONS,
    PARTITION_PROCESSOR_RATIO,
)


def _num_partitions_definition(num_processors, num_partitions):
    num_partitions = (
        num_processors * PARTITION_PROCESSOR_RATIO
        if num_processors
        else num_partitions or DEFAULT_NUM_PARTITIONS
    )

    return num_partitions


def repartition_df(
    dataframe: DataFrame, partition_by: List[str], num_partitions: int = None
):
    """Partition the DataFrame.

    Args:
        dataframe: Spark DataFrame.
        num_partitions: number of partitions.
        partition_by: list of partitions.

    Returns:
        Partitioned dataframe.

    """
    num_partitions = num_partitions or DEFAULT_NUM_PARTITIONS
    return dataframe.repartition(num_partitions, *partition_by)


def repartition_sort_df(
    dataframe: DataFrame,
    partition_by: List[str],
    order_by: List[str],
    num_processors: int = None,
    num_partitions: int = None,
):
    """Partition and Sort the DataFrame.

    Args:
        dataframe: Spark DataFrame.
        num_partitions: number of partitions.
        partition_by: list of partitions.
        num_processors: number of processors.

    Returns:
        Partitioned and sorted dataframe.

    """
    num_partitions = _num_partitions_definition(num_processors, num_partitions)
    dataframe = repartition_df(dataframe, partition_by, num_partitions)
    return dataframe.sortWithinPartitions(*order_by)
