from pyspark.sql.dataframe import DataFrame

from butterfree.core.constant.columns import TIMESTAMP_COLUMN


def verify_column_ts(dataframe: DataFrame):
    if TIMESTAMP_COLUMN not in dataframe.columns:
        raise ValueError("DataFrame must have a 'ts' column.")

    return dataframe
