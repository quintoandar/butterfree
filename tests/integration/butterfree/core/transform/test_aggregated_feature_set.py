from pyspark.sql import functions as F

from butterfree.core.clients import SparkClient
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import (
    AggregatedTransform,
    H3HashTransform,
)
from butterfree.testing.dataframe import assert_dataframe_equality


def divide(df, fs, column1, column2):
    name = fs.get_output_columns()[0]
    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


class TestAggregatedFeatureSet:
    def test_construct_without_window(
        self, feature_set_dataframe, target_df_without_window,
    ):
        # given

        spark_client = SparkClient()

        # arrange

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
                        functions=["avg"], group_by="id", column="feature1",
                    ),
                ),
                Feature(
                    name="feature2",
                    description="test",
                    dtype=DataType.BIGINT,
                    transformation=AggregatedTransform(
                        functions=["count"], group_by="id", column="feature2",
                    ),
                ),
            ],
            keys=[
                KeyFeature(
                    name="id",
                    description="The user's Main ID or device ID",
                    dtype=DataType.INTEGER,
                )
            ],
            timestamp=TimestampFeature(),
        )

        # act
        output_df = feature_set.construct(feature_set_dataframe, client=spark_client)

        target_df = target_df_without_window

        # assert
        assert_dataframe_equality(output_df, target_df)

    def test_construct_rolling_windows_with_end_date(
        self,
        feature_set_dataframe,
        rolling_windows_output_feature_set_dataframe_base_date,
    ):
        # given

        spark_client = SparkClient()

        # arrange

        feature_set = AggregatedFeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="test",
                    dtype=DataType.FLOAT,
                    transformation=AggregatedTransform(
                        functions=["avg", "stddev_pop"],
                        group_by="id",
                        column="feature1",
                    ).with_window(window_definition=["1 day"],),
                ),
                Feature(
                    name="feature2",
                    description="test",
                    dtype=DataType.FLOAT,
                    transformation=AggregatedTransform(
                        functions=["avg", "stddev_pop"],
                        group_by="id",
                        column="feature2",
                    ).with_window(window_definition=["1 week"],),
                ),
            ],
            keys=[
                KeyFeature(
                    name="id",
                    description="The user's Main ID or device ID",
                    dtype=DataType.INTEGER,
                )
            ],
            timestamp=TimestampFeature(),
        )

        # act
        output_df = feature_set.construct(
            feature_set_dataframe, client=spark_client, end_date="2016-04-18"
        ).orderBy("timestamp")

        target_df = rolling_windows_output_feature_set_dataframe_base_date.orderBy(
            feature_set.timestamp_column
        ).select(feature_set.columns)

        # assert
        assert_dataframe_equality(output_df, target_df)

    def test_construct_rolling_windows_without_end_date(
        self, feature_set_dataframe, rolling_windows_output_feature_set_dataframe
    ):
        # given

        spark_client = SparkClient()

        # arrange

        feature_set = AggregatedFeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="test",
                    dtype=DataType.FLOAT,
                    transformation=AggregatedTransform(
                        functions=["avg", "stddev_pop"],
                        group_by="id",
                        column="feature1",
                    ).with_window(window_definition=["1 day", "1 week"],),
                ),
            ],
            keys=[
                KeyFeature(
                    name="id",
                    description="The user's Main ID or device ID",
                    dtype=DataType.INTEGER,
                )
            ],
            timestamp=TimestampFeature(),
        )

        # act
        output_df = feature_set.construct(
            feature_set_dataframe, client=spark_client
        ).orderBy("timestamp")

        target_df = rolling_windows_output_feature_set_dataframe.orderBy(
            feature_set.timestamp_column
        ).select(feature_set.columns)

        # assert
        assert_dataframe_equality(output_df, target_df)

    def test_h3_feature_set(self, h3_input_df, h3_target_df):
        spark_client = SparkClient()

        feature_set = AggregatedFeatureSet(
            name="h3_test",
            entity="h3geolocation",
            description="Test",
            keys=[
                KeyFeature(
                    name="h3_id",
                    description="The h3 hash ID",
                    dtype=DataType.DOUBLE,
                    transformation=H3HashTransform(
                        h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
                        lat_column="lat",
                        lng_column="lng",
                    ).with_stack(),
                )
            ],
            timestamp=TimestampFeature(),
            features=[
                Feature(
                    name="house_id",
                    description="Count of house ids over a day.",
                    dtype=DataType.BIGINT,
                    transformation=AggregatedTransform(
                        functions=["count"], group_by="h3_id", column="house_id",
                    ).with_window(window_definition=["1 day"]),
                ),
            ],
        )

        output_df = feature_set.construct(h3_input_df, client=spark_client)

        assert_dataframe_equality(output_df, h3_target_df)
