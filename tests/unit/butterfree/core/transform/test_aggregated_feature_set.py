import pytest
from pyspark.sql import functions
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, TimestampType

from butterfree.core.clients import SparkClient
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import (
    AggregatedTransform,
    SparkFunctionTransform,
)


class TestFeatureSet:
    def test_feature_set_with_invalid_feature(self, key_id, timestamp_c, dataframe):
        spark_client = SparkClient()

        with pytest.raises(ValueError):
            AggregatedFeatureSet(
                name="name",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature1",
                        description="test",
                        dtype=DataType.FLOAT,
                        transformation=SparkFunctionTransform(
                            functions=[functions.avg],
                        ).with_window(
                            partition_by="id",
                            mode="row_windows",
                            window_definition=["2 events"],
                        ),
                    ),
                ],
                keys=[key_id],
                timestamp=timestamp_c,
            ).construct(dataframe, spark_client)

    def test_feature_set_with_window_and_no_window(
        self, key_id, timestamp_c, dataframe
    ):
        spark_client = SparkClient()

        with pytest.raises(ValueError):
            AggregatedFeatureSet(
                name="name",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature1",
                        description="unit test",
                        dtype=DataType.FLOAT,
                        transformation=AggregatedTransform(
                            functions=["avg"], group_by="id", column="feature1",
                        ).with_window(window_definition=["1 day", "1 week"]),
                    ),
                    Feature(
                        name="feature1",
                        dtype=DataType.FLOAT,
                        description="unit test",
                        transformation=AggregatedTransform(
                            functions=["count"], group_by="id", column="feature1",
                        ),
                    ),
                ],
                keys=[key_id],
                timestamp=timestamp_c,
            ).construct(dataframe, spark_client)

    def test_get_schema(self):
        expected_schema = [
            {"column_name": "id", "type": LongType(), "primary_key": True},
            {"column_name": "timestamp", "type": TimestampType(), "primary_key": False},
            {
                "column_name": "feature1__avg_over_1_week_rolling_windows",
                "type": DoubleType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__stddev_pop_over_1_week_rolling_windows",
                "type": DoubleType(),
                "primary_key": False,
            },
            {
                "column_name": "feature2__count_over_2_days_rolling_windows",
                "type": ArrayType(StringType(), True),
                "primary_key": False,
            },
        ]

        feature_set = AggregatedFeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="test",
                    dtype=DataType.DOUBLE,
                    transformation=AggregatedTransform(
                        functions=["avg", "stddev_pop"],
                        group_by="id",
                        column="feature1",
                    ).with_window(window_definition=["1 week"]),
                ),
                Feature(
                    name="feature2",
                    description="test",
                    dtype=DataType.ARRAY_STRING,
                    transformation=AggregatedTransform(
                        functions=["count"], group_by="id", column="feature2",
                    ).with_window(window_definition=["2 days"]),
                ),
            ],
            keys=[
                KeyFeature(
                    name="id",
                    description="The user's Main ID or device ID",
                    dtype=DataType.BIGINT,
                )
            ],
            timestamp=TimestampFeature(),
        )

        schema = feature_set.get_schema()

        assert schema == expected_schema
