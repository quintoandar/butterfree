import datetime
import random

import pytest


@pytest.fixture()
def input_df(spark_context, spark_session):
    start = datetime.datetime(year=1970, month=1, day=1)
    end = datetime.datetime(year=2020, month=12, day=31)
    random_dates = [
        (
            lambda: start
            + datetime.timedelta(
                seconds=random.randint(  # noqa: S311
                    0, int((end - start).total_seconds())
                )
            )
        )()
        .date()
        .isoformat()
        for _ in range(10000)
    ]
    data = [{"timestamp": date} for date in random_dates]
    return spark_session.read.json(
        spark_context.parallelize(data, 1), schema="timestamp timestamp"
    )


@pytest.fixture()
def test_partitioning_input_df(spark_context, spark_session):
    data = [
        {"feature": 1, "year": 2009, "month": 8, "day": 20},
        {"feature": 2, "year": 2009, "month": 8, "day": 20},
        {"feature": 3, "year": 2020, "month": 8, "day": 20},
        {"feature": 4, "year": 2020, "month": 9, "day": 20},
        {"feature": 5, "year": 2020, "month": 9, "day": 20},
        {"feature": 6, "year": 2020, "month": 8, "day": 20},
        {"feature": 7, "year": 2020, "month": 8, "day": 21},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))
