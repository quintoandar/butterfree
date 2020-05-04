from pytest import fixture


@fixture
def feature_set_dataframe(spark_context, spark_session):
    data = [
        {"id": 1, "feature1": 100},
        {"id": 1, "feature1": 100},
        {"id": 1, "feature1": 200},
        {"id": 1, "feature1": 200},
        {"id": 1, "feature1": 200},
        {"id": 1, "feature1": 300},
        {"id": 1, "feature1": 300},
        {"id": 1, "feature1": 300},
        {"id": 1, "feature1": 300},
        {"id": 1, "feature1": 300},
        {"id": 2, "feature1": 100},
        {"id": 2, "feature1": 100},
        {"id": 2, "feature1": 200},
        {"id": 2, "feature1": 200},
        {"id": 2, "feature1": 200},
        {"id": 2, "feature1": 300},
        {"id": 2, "feature1": 300},
        {"id": 2, "feature1": 300},
        {"id": 2, "feature1": 300},
        {"id": 2, "feature1": 300},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def mode_target_df(spark_context, spark_session):
    data = [
        {"id": 1, "mode(feature1)": "300"},
        {"id": 2, "mode(feature1)": "300"},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))


@fixture
def most_frequent_target_df(spark_context, spark_session):
    data = [
        {"id": 1, "most_frequent_elements_list(feature1)": [300, 200, 100]},
        {"id": 2, "most_frequent_elements_list(feature1)": [300, 200, 100]},
    ]
    return spark_session.read.json(spark_context.parallelize(data, 1))
