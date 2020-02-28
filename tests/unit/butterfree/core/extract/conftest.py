import json
from unittest.mock import Mock

from pytest import fixture


@fixture
def column_target_df(spark_context, spark_session):
    data = [
        {"new_feature1": 100, "new_feature2": 100},
        {"new_feature1": 200, "new_feature2": 200},
        {"new_feature1": 300, "new_feature2": 300},
        {"new_feature1": 400, "new_feature2": 400},
    ]
    return spark_session.read.json(
        spark_context.parallelize(data).map(lambda x: json.dumps(x))
    )


@fixture()
def spark_client():
    return Mock()
