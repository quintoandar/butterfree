from pyspark.sql import functions as F

from butterfree.core.clients import SparkClient
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import (
    AggregatedTransform,
    CustomTransform,
)


def divide(df, fs, column1, column2):
    name = fs.get_output_columns()[0]
    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


class TestFeatureSet:
    def test_construct(
        self, feature_set_dataframe, fixed_windows_output_feature_set_dataframe
    ):
        # given

        spark_client = SparkClient()

        # arrange

        feature_set = FeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="test",
                    transformation=AggregatedTransform(
                        aggregations=["avg", "stddev_pop"],
                        partition="id",
                        windows_agg=["2 minutes", "15 minutes"],
                        mode=["fixed_windows"],
                    ),
                ),
                Feature(
                    name="divided_feature",
                    description="unit test",
                    transformation=CustomTransform(
                        transformer=divide, column1="feature1", column2="feature2",
                    ),
                ),
            ],
            keys=[KeyFeature(name="id", description="The user's Main ID or device ID")],
            timestamp=TimestampFeature(),
        )

        result_df = (
            feature_set.construct(feature_set_dataframe, client=spark_client)
            .orderBy(feature_set.timestamp_column)
            .select(feature_set.columns)
            .collect()
        )

        # assert
        assert (
            result_df
            == fixed_windows_output_feature_set_dataframe.orderBy(
                feature_set.timestamp_column
            )
            .select(feature_set.columns)
            .collect()
        )

    def test_construct_rolling_windows_with_base_date(
        self,
        feature_set_dataframe,
        rolling_windows_output_feature_set_dataframe_base_date,
    ):
        # given

        spark_client = SparkClient()

        # arrange

        feature_set = FeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="test",
                    transformation=AggregatedTransform(
                        aggregations=["avg", "stddev_pop"],
                        partition="id",
                        windows_agg=["1 day", "1 week"],
                        mode=["rolling_windows"],
                    ),
                ),
            ],
            keys=[KeyFeature(name="id", description="The user's Main ID or device ID")],
            timestamp=TimestampFeature(),
        )

        # act
        result_df = (
            feature_set.construct(
                feature_set_dataframe, client=spark_client, base_date="2016-04-18"
            )
            .orderBy("timestamp")
            .collect()
        )

        # assert
        assert (
            result_df
            == rolling_windows_output_feature_set_dataframe_base_date.orderBy(
                feature_set.timestamp_column
            )
            .select(feature_set.columns)
            .collect()
        )

    def test_construct_rolling_windows_without_base_date(
        self, feature_set_dataframe, rolling_windows_output_feature_set_dataframe
    ):
        # given

        spark_client = SparkClient()

        # arrange

        feature_set = FeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="test",
                    transformation=AggregatedTransform(
                        aggregations=["avg", "stddev_pop"],
                        partition="id",
                        windows_agg=["1 day", "1 week"],
                        mode=["rolling_windows"],
                    ),
                ),
            ],
            keys=[KeyFeature(name="id", description="The user's Main ID or device ID")],
            timestamp=TimestampFeature(),
        )

        # act
        result_df = (
            feature_set.construct(feature_set_dataframe, client=spark_client)
            .orderBy("timestamp")
            .collect()
        )

        # assert
        assert (
            result_df
            == rolling_windows_output_feature_set_dataframe.orderBy(
                feature_set.timestamp_column
            )
            .select(feature_set.columns)
            .collect()
        )
