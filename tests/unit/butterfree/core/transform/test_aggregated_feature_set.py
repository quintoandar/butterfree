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
from butterfree.testing.dataframe import (
    assert_dataframe_equality,
    create_df_from_collection,
)


class TestAggregatedFeatureSet:
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

    def test_agg_feature_set_with_window(
        self, key_id, timestamp_c, dataframe, rolling_windows_agg_dataframe
    ):
        spark_client = SparkClient()

        fs = AggregatedFeatureSet(
            name="name",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="unit test",
                    dtype=DataType.FLOAT,
                    transformation=AggregatedTransform(functions=["avg"]),
                ),
                Feature(
                    name="feature2",
                    dtype=DataType.FLOAT,
                    description="unit test",
                    transformation=AggregatedTransform(functions=["avg"]),
                ),
            ],
            keys=[key_id],
            timestamp=timestamp_c,
        ).with_windows(definitions=["1 week"])

        # raises without end date
        with pytest.raises(ValueError):
            _ = fs.construct(dataframe, spark_client)

        # filters with date smaller then mocked max
        output_df = fs.construct(dataframe, spark_client, end_date="2016-04-17")
        assert output_df.count() < rolling_windows_agg_dataframe.count()
        output_df = fs.construct(dataframe, spark_client, end_date="2016-05-01")
        assert_dataframe_equality(output_df, rolling_windows_agg_dataframe)

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
                "column_name": "feature1__avg_over_2_days_rolling_windows",
                "type": DoubleType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__stddev_pop_over_1_week_rolling_windows",
                "type": DoubleType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__stddev_pop_over_2_days_rolling_windows",
                "type": DoubleType(),
                "primary_key": False,
            },
            {
                "column_name": "feature2__count_over_1_week_rolling_windows",
                "type": ArrayType(StringType(), True),
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
                    ),
                ),
                Feature(
                    name="feature2",
                    description="test",
                    dtype=DataType.ARRAY_STRING,
                    transformation=AggregatedTransform(functions=["count"]),
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
        ).with_windows(definitions=["1 week", "2 days"])

        schema = feature_set.get_schema()

        assert schema == expected_schema

    def test_feature_transform_with_distinct(
        self,
        timestamp_c,
        feature_set_with_distinct_dataframe,
        target_with_distinct_dataframe,
    ):
        spark_client = SparkClient()

        fs = (
            AggregatedFeatureSet(
                name="name",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature",
                        description="test",
                        dtype=DataType.INTEGER,
                        transformation=AggregatedTransform(functions=["sum"]),
                    ),
                ],
                keys=[KeyFeature(name="h3", description="test", dtype=DataType.STRING)],
                timestamp=timestamp_c,
            )
            .with_windows(["3 days"])
            .with_distinct(subset=["id"], keep="last")
        )

        # assert
        output_df = fs.construct(
            feature_set_with_distinct_dataframe, spark_client, end_date="2020-01-05"
        )
        assert_dataframe_equality(output_df, target_with_distinct_dataframe)

    def test_feature_transform_with_distinct_invalid_keep(
        self, timestamp_c, feature_set_with_distinct_dataframe
    ):
        spark_client = SparkClient()

        with pytest.raises(
            ValueError, match="The distinct keep param can only be 'last' or 'first'."
        ):
            AggregatedFeatureSet(
                name="name",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature",
                        description="test",
                        dtype=DataType.INTEGER,
                        transformation=AggregatedTransform(functions=["sum"]),
                    ),
                ],
                keys=[KeyFeature(name="h3", description="test", dtype=DataType.STRING)],
                timestamp=timestamp_c,
            ).with_windows(["3 days"]).with_distinct(
                subset=["id"], keep="keep"
            ).construct(
                feature_set_with_distinct_dataframe, spark_client, end_date="2020-01-10"
            )

    def test_feature_transform_with_distinct_empty_subset(
        self, timestamp_c, feature_set_with_distinct_dataframe
    ):
        spark_client = SparkClient()

        with pytest.raises(
            ValueError, match="The distinct subset param can't be empty."
        ):
            AggregatedFeatureSet(
                name="name",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature",
                        description="test",
                        dtype=DataType.INTEGER,
                        transformation=AggregatedTransform(functions=["sum"]),
                    ),
                ],
                keys=[KeyFeature(name="h3", description="test", dtype=DataType.STRING)],
                timestamp=timestamp_c,
            ).with_windows(["3 days"]).with_distinct(subset=[], keep="first").construct(
                feature_set_with_distinct_dataframe, spark_client, end_date="2020-01-10"
            )

    def test_feature_transform_with_filter_expression(
        self, spark_context, spark_session
    ):
        # arrange
        input_data = [
            {
                "id": 1,
                "timestamp": "2020-04-22T00:00:00+00:00",
                "feature": 10,
                "type": "a",
            },
            {
                "id": 1,
                "timestamp": "2020-04-22T00:00:00+00:00",
                "feature": 20,
                "type": "a",
            },
            {
                "id": 1,
                "timestamp": "2020-04-22T00:00:00+00:00",
                "feature": 30,
                "type": "b",
            },
            {
                "id": 2,
                "timestamp": "2020-04-22T00:00:00+00:00",
                "feature": 10,
                "type": "a",
            },
        ]
        target_data = [
            {
                "id": 1,
                "timestamp": "2020-04-22T00:00:00+00:00",
                "feature_only_type_a__avg": 15.0,
                "feature_only_type_a__min": 10,
                "feature_only_type_a__max": 20,
            },
            {
                "id": 2,
                "timestamp": "2020-04-22T00:00:00+00:00",
                "feature_only_type_a__avg": 10.0,
                "feature_only_type_a__min": 10,
                "feature_only_type_a__max": 10,
            },
        ]
        input_df = create_df_from_collection(
            input_data, spark_context, spark_session
        ).withColumn("timestamp", functions.to_timestamp(functions.col("timestamp")))
        target_df = create_df_from_collection(
            target_data, spark_context, spark_session
        ).withColumn("timestamp", functions.to_timestamp(functions.col("timestamp")))

        fs = AggregatedFeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[KeyFeature(name="id", description="test", dtype=DataType.INTEGER)],
            timestamp=TimestampFeature(),
            features=[
                Feature(
                    name="feature_only_type_a",
                    description="aggregations only when type = a",
                    dtype=DataType.BIGINT,
                    transformation=AggregatedTransform(
                        functions=["avg", "min", "max"], filter_expression="type = 'a'"
                    ),
                    from_column="feature",
                ),
            ],
        )

        # act
        output_df = fs.construct(input_df, SparkClient())

        # assert
        assert_dataframe_equality(target_df, output_df)
