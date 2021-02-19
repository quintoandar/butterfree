import pytest
from pyspark.sql import functions
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    TimestampType,
)

from butterfree.clients import SparkClient
from butterfree.constants import DataType
from butterfree.testing.dataframe import (
    assert_dataframe_equality,
    create_df_from_collection,
)
from butterfree.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import (
    AggregatedTransform,
    SparkFunctionTransform,
)
from butterfree.transform.utils import Function


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
                        transformation=SparkFunctionTransform(
                            functions=[Function(functions.avg, DataType.FLOAT)],
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
        self,
        key_id,
        timestamp_c,
        dataframe,
        rolling_windows_agg_dataframe,
        agg_feature_set,
    ):
        spark_client = SparkClient()

        fs = agg_feature_set.with_windows(definitions=["1 week"])

        # raises without end date
        with pytest.raises(ValueError):
            _ = fs.construct(dataframe, spark_client)

        # filters with date smaller then mocked max
        output_df = fs.construct(dataframe, spark_client, end_date="2016-04-17")
        assert output_df.count() < rolling_windows_agg_dataframe.count()
        output_df = fs.construct(dataframe, spark_client, end_date="2016-05-01")
        assert_dataframe_equality(output_df, rolling_windows_agg_dataframe)

    def test_agg_feature_set_with_smaller_slide(
        self,
        key_id,
        timestamp_c,
        dataframe,
        rolling_windows_hour_slide_agg_dataframe,
        agg_feature_set,
    ):
        spark_client = SparkClient()

        fs = agg_feature_set.with_windows(definitions=["1 day"], slide="12 hours")

        # raises without end date
        with pytest.raises(ValueError):
            _ = fs.construct(dataframe, spark_client)

        # filters with date smaller then mocked max
        output_df = fs.construct(dataframe, spark_client, end_date="2016-04-17")
        assert_dataframe_equality(output_df, rolling_windows_hour_slide_agg_dataframe)

    def test_get_schema(self, agg_feature_set):
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
                "type": FloatType(),
                "primary_key": False,
            },
            {
                "column_name": "feature1__stddev_pop_over_2_days_rolling_windows",
                "type": FloatType(),
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

        schema = agg_feature_set.get_schema()

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
                        transformation=AggregatedTransform(
                            functions=[Function(functions.sum, DataType.INTEGER)]
                        ),
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
            feature_set_with_distinct_dataframe, spark_client, end_date="2020-01-10"
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
                        transformation=AggregatedTransform(
                            functions=[Function(functions.sum, DataType.INTEGER)]
                        ),
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
                        transformation=AggregatedTransform(
                            functions=[Function(functions.sum, DataType.INTEGER)]
                        ),
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
                        functions=[
                            Function(functions.avg, DataType.FLOAT),
                            Function(functions.min, DataType.FLOAT),
                            Function(functions.max, DataType.FLOAT),
                        ],
                        filter_expression="type = 'a'",
                    ),
                    from_column="feature",
                ),
            ],
        )

        # act
        output_df = fs.construct(input_df, SparkClient())

        # assert
        assert_dataframe_equality(target_df, output_df)

    def test_feature_transform_with_data_type_array(self, spark_context, spark_session):
        # arrange
        input_data = [
            {"id": 1, "timestamp": "2020-04-22T00:00:00+00:00", "feature": 10},
            {"id": 1, "timestamp": "2020-04-22T00:00:00+00:00", "feature": 20},
            {"id": 1, "timestamp": "2020-04-22T00:00:00+00:00", "feature": 30},
            {"id": 2, "timestamp": "2020-04-22T00:00:00+00:00", "feature": 10},
        ]
        target_data = [
            {
                "id": 1,
                "timestamp": "2020-04-22T00:00:00+00:00",
                "feature__collect_set": [30.0, 20.0, 10.0],
            },
            {
                "id": 2,
                "timestamp": "2020-04-22T00:00:00+00:00",
                "feature__collect_set": [10.0],
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
                    name="feature",
                    description="aggregations with ",
                    dtype=DataType.BIGINT,
                    transformation=AggregatedTransform(
                        functions=[
                            Function(functions.collect_set, DataType.ARRAY_FLOAT),
                        ],
                    ),
                    from_column="feature",
                ),
            ],
        )

        # act
        output_df = fs.construct(input_df, SparkClient())

        # assert
        assert_dataframe_equality(target_df, output_df)

    def test_define_start_date(self, agg_feature_set):
        start_date = agg_feature_set.define_start_date("2020-08-04")

        assert isinstance(start_date, str)
        assert start_date == "2020-07-27"

    def test_feature_set_start_date(
        self, timestamp_c, feature_set_with_distinct_dataframe,
    ):
        fs = AggregatedFeatureSet(
            name="name",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature",
                    description="test",
                    transformation=AggregatedTransform(
                        functions=[Function(functions.sum, DataType.INTEGER)]
                    ),
                ),
            ],
            keys=[KeyFeature(name="h3", description="test", dtype=DataType.STRING)],
            timestamp=timestamp_c,
        ).with_windows(["10 days", "3 weeks", "90 days"])

        # assert
        start_date = fs.define_start_date("2016-04-14")

        assert start_date == "2016-01-14"
