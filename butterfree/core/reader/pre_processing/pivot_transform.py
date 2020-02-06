"""Pivot Transform for Readers."""
from typing import List

from parameters_validation import non_blank
from pyspark.sql import Column, DataFrame


def pivot_table(
    dataframe: DataFrame,
    group_by_columns: non_blank(List[str]),
    pivot_column: non_blank(str),
    aggregation_expression: non_blank(Column),
):
    """Defines a pivot transformation.

    Attributes:
        dataframe: dataframe to be pivoted.
        group_by_columns: list of columns' names to be grouped.
        pivot_column: column to be pivoted.
        aggregation_expression: desired aggregation to be performed with a column.

    """
    return (
        dataframe.groupBy(*group_by_columns)
        .pivot(pivot_column)
        .agg(aggregation_expression)
    )
