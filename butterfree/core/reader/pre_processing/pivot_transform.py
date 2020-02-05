from pyspark.sql import DataFrame


def pivot_table(
    dataframe: DataFrame, group_by_columns, pivot_column, aggregation, agg_column
):
    return (
        dataframe.groupBy(*group_by_columns)
        .pivot(pivot_column)
        .agg(aggregation(agg_column))
    )
