import pytest

from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import AggregatedTransform
from butterfree.testing.dataframe import assert_dataframe_equality


class TestAggregatedTransform:
    def test_feature_transform(self, feature_set_dataframe, target_df_agg):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            dtype=DataType.BIGINT,
            transformation=AggregatedTransform(
                functions=["avg", "stddev_pop"], group_by=["id"], column="feature1",
            ),
        )

        output_df = test_feature.transform(feature_set_dataframe)

        assert_dataframe_equality(output_df, target_df_agg)

    def test_output_columns(self):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            dtype=DataType.BIGINT,
            transformation=AggregatedTransform(
                functions=["avg", "stddev_pop"], group_by="id", column="feature1",
            ).with_window(window_definition=["7 days", "2 weeks"]),
        )

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(
                    df_columns,
                    [
                        "feature1__avg_over_7_days_rolling_windows",
                        "feature1__avg_over_2_weeks_rolling_windows",
                        "feature1__stddev_pop_over_7_days_rolling_windows",
                        "feature1__stddev_pop_over_2_weeks_rolling_windows",
                    ],
                )
            ]
        )

    def test_unsupported_aggregation(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                dtype=DataType.BIGINT,
                transformation=AggregatedTransform(
                    functions=["median"], group_by="id", column="feature1",
                ).with_window(window_definition=["7 days", "2 weeks"]),
            )

    def test_blank_aggregation(self, feature_set_dataframe):
        with pytest.raises(ValueError, match="Aggregations must not be empty."):
            Feature(
                name="feature1",
                description="unit test",
                dtype=DataType.BIGINT,
                transformation=AggregatedTransform(
                    functions=[], group_by="id", column="feature1",
                ),
            )

    def test_negative_window(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                dtype=DataType.BIGINT,
                transformation=AggregatedTransform(
                    functions=["avg", "stddev_pop"], group_by="id", column="feature1",
                ).with_window(window_definition=["-2 weeks"]),
            ).transform(feature_set_dataframe)

    def test_feature_transform_output_rolling_windows(
        self, feature_set_dataframe, target_df_rolling_agg
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            dtype=DataType.DOUBLE,
            transformation=AggregatedTransform(
                functions=["avg", "stddev_pop", "count"],
                group_by="id",
                column="feature1",
            ).with_window(window_definition=["1 day", "1 week"]),
        )

        output_df = test_feature.transform(feature_set_dataframe).orderBy("timestamp")

        assert_dataframe_equality(output_df, target_df_rolling_agg)
