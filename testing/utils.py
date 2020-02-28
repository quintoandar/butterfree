"""Useful testing methods."""
from typing import List

from pyspark.sql import DataFrame


def check_dataframe_equality(
    output_df: DataFrame, target_df: DataFrame, columns_sort: List[str] = None
):
    """Dataframe comparison method."""
    if not (
        output_df.count() == target_df.count()
        and len(target_df.columns) == len(output_df.columns)
    ):
        raise AssertionError(
            f"DataFrame shape mismatch: "
            f"output_df shape: {len(output_df.columns)} columns and "
            f"{output_df.count()} lines, "
            f"target_df shape: {len(target_df.columns)} columns and "
            f"{target_df.count()} lines."
        )

    if not columns_sort:
        columns_sort = output_df.schema.fieldNames()

    # select_cols = [col(c).cast("string") for c in columns_sort]

    print(
        "output_df: ", sorted(output_df.select(*columns_sort).collect()),
    )
    print("target_df: ", sorted(target_df.select(*columns_sort).collect()))
    return sorted(output_df.select(*columns_sort).collect()) == sorted(
        target_df.select(*columns_sort).collect()
    )
