"""Holds common aggregated types."""
from pyspark.sql import functions

ALLOWED_AGGREGATIONS = {
    "approx_count_distinct": functions.approx_count_distinct,
    "avg": functions.avg,
    "collect_list": functions.collect_list,
    "collect_set": functions.collect_set,
    "count": functions.count,
    "first": functions.first,
    "kurtosis": functions.kurtosis,
    "last": functions.first,
    "max": functions.max,
    "min": functions.min,
    "skewness": functions.skewness,
    "stddev": functions.stddev,
    "stddev_pop": functions.stddev_pop,
    "sum": functions.sum,
    "sum_distinct": functions.sumDistinct,
    "variance": functions.variance,
    "var_pop": functions.var_pop,
}
