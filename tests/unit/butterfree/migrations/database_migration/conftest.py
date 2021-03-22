from pyspark.sql.types import DoubleType, FloatType, LongType, TimestampType
from pytest import fixture

from butterfree.constants import DataType
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature


@fixture
def db_schema():
    return [
        {"column_name": "id", "type": LongType(), "primary_key": True},
        {"column_name": "timestamp", "type": TimestampType(), "primary_key": False},
        {
            "column_name": "feature1__avg_over_1_week_rolling_windows",
            "type": DoubleType(),
            "primary_key": False,
        },
        {
            "column_name": "feature1__avg_over_2_days_rolling_windows",
            "type": DoubleType(),
            "primary_key": False,
        },
    ]


@fixture
def fs_schema():
    return [
        {"column_name": "id", "type": LongType(), "primary_key": True},
        {"column_name": "timestamp", "type": TimestampType(), "primary_key": True},
        {"column_name": "new_feature", "type": FloatType(), "primary_key": False},
        {
            "column_name": "feature1__avg_over_1_week_rolling_windows",
            "type": FloatType(),
            "primary_key": False,
        },
    ]


@fixture
def feature_set():
    feature_set = FeatureSet(
        name="feature_set",
        entity="entity",
        description="description",
        features=[
            Feature(name="feature_float", description="test", dtype=DataType.FLOAT,),
        ],
        keys=[
            KeyFeature(name="id", description="The device ID", dtype=DataType.BIGINT,)
        ],
        timestamp=TimestampFeature(),
    )

    return feature_set
