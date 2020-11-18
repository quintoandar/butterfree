import json

import pytest


@pytest.fixture()
def input_df(spark_context, spark_session):
    data = [
        {"id": 1, "ts": "2016-04-11 11:31:11"},
        {"id": 1, "ts": "2016-04-11 11:44:12"},
        {"id": 1, "ts": "2016-04-11 11:46:24"},
        {"id": 1, "ts": "2016-04-11 12:03:21"},
        {"id": 1, "ts": "2016-04-11 13:46:24"},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@pytest.fixture()
def json_df(spark_context, spark_session):
    data = [
        '{"value":"{\\"id\\":1,\\"ts\\":\\"2016-04-11 11:31:11\\"}"}',
        '{"value":"{\\"id\\":1,\\"ts\\":\\"2016-04-11 11:44:12\\"}"}',
        '{"value":"{\\"id\\":1,\\"ts\\":\\"2016-04-11 11:46:24\\"}"}',
        '{"value":"{\\"id\\":1,\\"ts\\":\\"2016-04-11 12:03:21\\"}"}',
        '{"value":"{\\"id\\":1,\\"ts\\":\\"2016-04-11 13:46:24\\"}"}',
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))
