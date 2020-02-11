from pyspark.sql import functions as F

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
    def test_construct(self, feature_set_dataframe):
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
                        windows=["2 minutes", "15 minutes"],
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

        # act
        df = feature_set.construct(feature_set_dataframe).collect()

        # assert
        assert df[0]["feature1__avg_over_2_minutes_fixed_windows"] == 200
        assert df[1]["feature1__avg_over_2_minutes_fixed_windows"] == 300
        assert df[2]["feature1__avg_over_2_minutes_fixed_windows"] == 400
        assert df[3]["feature1__avg_over_2_minutes_fixed_windows"] == 500
        assert df[0]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[1]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[2]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[3]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[0]["feature1__avg_over_15_minutes_fixed_windows"] == 200
        assert df[1]["feature1__avg_over_15_minutes_fixed_windows"] == 250
        assert df[2]["feature1__avg_over_15_minutes_fixed_windows"] == 350
        assert df[3]["feature1__avg_over_15_minutes_fixed_windows"] == 500
        assert df[0]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 0
        assert df[1]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 50
        assert df[2]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 50
        assert df[3]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 0
        assert df[0]["divided_feature"] == 1
        assert df[1]["divided_feature"] == 1
        assert df[2]["divided_feature"] == 1
        assert df[3]["divided_feature"] == 1
