"""Module where there are repartition methods."""
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.constants.spark_constants import (
    DEFAULT_NUM_PARTITIONS,
    PARTITION_PROCESSOR_RATIO,
)


def _num_partitions_definition(
    num_processors: int = None, num_partitions: int = None
) -> int:
    num_partitions = (
        num_processors * PARTITION_PROCESSOR_RATIO
        if num_processors
        else num_partitions or DEFAULT_NUM_PARTITIONS
    )

    return num_partitions


def repartition_df(
    dataframe: DataFrame,
    partition_by: List[str],
    num_partitions: int = None,
    num_processors: int = None,
) -> DataFrame:
    """Partition the DataFrame.

    Args:
        dataframe: Spark DataFrame.
        partition_by: list of partitions.
        num_processors: number of processors.
        num_partitions: number of partitions.

    Returns:
        Partitioned dataframe.

    """
    num_partitions = _num_partitions_definition(num_processors, num_partitions)
    return dataframe.repartition(num_partitions, *partition_by)


def repartition_sort_df(
    dataframe: DataFrame,
    partition_by: List[str],
    order_by: List[str],
    num_processors: int = None,
    num_partitions: int = None,
) -> DataFrame:
    """Partition and Sort the DataFrame.

    Args:
        dataframe: Spark DataFrame.
        partition_by: list of columns to partition by.
        order_by: list of columns to order by.
        num_processors: number of processors.
        num_partitions: number of partitions.

    Returns:
        Partitioned and sorted dataframe.

    """
    num_partitions = _num_partitions_definition(num_processors, num_partitions)
    dataframe = repartition_df(dataframe, partition_by, num_partitions)
    return dataframe.sortWithinPartitions(*order_by)
