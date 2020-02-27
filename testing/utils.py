"""Useful testing methods."""
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def compare_dataframes(
    output_df: DataFrame, target_df: DataFrame, columns_sort: List[str] = None
):
    """Dataframe comparison method."""
    if not (
        output_df.count() == target_df.count()
        and len(target_df.columns) == len(output_df.columns)
    ):
        raise ValueError(
            f"DataFrame shape mismatch: "
            f"output_df shape: {len(output_df.columns), output_df.count()}, "
            f"target_df shape: {len(target_df.columns), target_df.count()}"
        )

    if not columns_sort:
        columns_sort = target_df.schema.fieldNames()

    select_cols = [col(c).cast("string") for c in columns_sort]

    print(
        "output_df: ", sorted(output_df.select(*select_cols).na.fill("None").collect()),
    )
    print("target_df: ", sorted(target_df.select(*select_cols).collect()))
    return sorted(output_df.select(*select_cols).na.fill("None").collect()) == sorted(
        target_df.select(*select_cols).collect()
    )
