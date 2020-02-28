"""Methods to assert properties regarding Apache Spark Dataframes."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def assert_dataframe_equality(output_df: DataFrame, target_df: DataFrame):
    """Dataframe comparison method."""
    if not (
        output_df.count() == target_df.count()
        and len(target_df.columns) == len(output_df.columns)
    ):
        raise AssertionError(
            f"DataFrame shape mismatch: \n"
            f"output_df shape: {len(output_df.columns)} columns and "
            f"{output_df.count()} rows\n"
            f"target_df shape: {len(target_df.columns)} columns and "
            f"{target_df.count()} rows."
        )

    select_cols = [col(c) for c in output_df.schema.fieldNames()]
    output_data = sorted(output_df.select(*select_cols).collect())
    target_data = sorted(target_df.select(*select_cols).collect())
    if not output_data == target_data:
        raise AssertionError(
            "DataFrames have different values:\n"
            f"output_df records: {output_data}\n"
            f"target_data records: {target_data}."
        )
