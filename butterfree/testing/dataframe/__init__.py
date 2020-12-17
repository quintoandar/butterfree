"""Methods to assert properties regarding Apache Spark Dataframes."""
from json import dumps
from typing import Any, Dict, List

from pyspark import SparkContext
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType


def assert_dataframe_equality(output_df: DataFrame, target_df: DataFrame) -> None:
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
    output_data = [row.asDict(recursive=True) for row in output_data]  # type: ignore

    target_data = sorted(target_df.select(*select_cols).collect())
    target_data = [row.asDict(recursive=True) for row in target_data]  # type: ignore

    if not output_data == target_data:
        raise AssertionError(
            "DataFrames have different values:\n"
            f"output_df records: {output_data}\n"
            f"target_data records: {target_data}."
        )


def assert_column_equality(
    output_df: DataFrame,
    target_df: DataFrame,
    output_column: Column,
    target_column: Column,
) -> None:
    """Columns comparison method."""
    if not (
        output_df.select(output_column).count()
        == target_df.select(target_column).count()
        and len(target_df.columns) == len(output_df.columns)
    ):
        raise AssertionError(
            f"DataFrame shape mismatch: \n"
            f"output_df shape: {len(output_df.columns)} columns and "
            f"{output_df.count()} rows\n"
            f"target_df shape: {len(target_df.columns)} columns and "
            f"{target_df.count()} rows."
        )

    output_data = output_df.selectExpr(f"{output_column} as {target_column}").collect()
    target_data = target_df.select(target_column).collect()
    if not output_data == target_data:
        raise AssertionError(
            "Columns have different values:\n"
            f"output_column records: {output_data}\n"
            f"target_column records: {target_data}."
        )


def create_df_from_collection(
    data: List[Dict[Any, Any]],
    spark_context: SparkContext,
    spark_session: SparkSession,
    schema: StructType = None,
) -> DataFrame:
    """Creates a dataframe from a list of dicts."""
    return spark_session.read.json(
        spark_context.parallelize(data, 1).map(lambda x: dumps(x)), schema=schema
    )
