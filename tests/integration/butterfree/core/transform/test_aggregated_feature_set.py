from pyspark.sql import functions as F

from butterfree.core.clients import SparkClient
from butterfree.core.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import AggregatedTransform


def divide(df, fs, column1, column2):
    name = fs.get_output_columns()[0]
    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


class TestAggregatedFeatureSet:
    def test_construct_rolling_windows_with_base_date(
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
                    transformation=AggregatedTransform(
                        functions=["avg", "stddev_pop"],
                        group_by="id",
                        column="feature1",
                    ).with_window(window_definition=["1 day"],),
                ),
                Feature(
                    name="feature1",
                    description="test",
                    transformation=AggregatedTransform(
                        functions=["avg", "stddev_pop"],
                        group_by="id",
                        column="feature1",
                    ).with_window(window_definition=["1 week"],),
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

        feature_set = AggregatedFeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="test",
                    transformation=AggregatedTransform(
                        functions=["avg", "stddev_pop"],
                        group_by="id",
                        column="feature1",
                    ).with_window(window_definition=["1 day", "1 week"],),
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
