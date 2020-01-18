from pyspark.sql import functions as F


def get_agg(aggregation, feature_name):
    if aggregation in ["avg"]:
        return F.avg(feature_name)
    elif aggregation in ["std"]:
        return F.stddev_pop(feature_name)
    else:
        raise ValueError()
