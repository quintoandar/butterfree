from pyspark.sql import DataFrame
from parameters_validation import parameter_validation


@parameter_validation
def verify_column_ts(dataframe: DataFrame):
  if "ts" not in dataframe.columns:
    raise ValueError('DataFrame must have a ts column')

  return dataframe
