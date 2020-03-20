import pytest

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import SparkFunctionTransform
from butterfree.core.transform.utils import with_window
from butterfree.testing.dataframe import assert_dataframe_equality


class TestSparkFunctionTransform:
    def test_feature_transform(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=SparkFunctionTransform(functions=["avg"],),
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    df.columns,
                    ["feature1", "feature2", "id", TIMESTAMP_COLUMN, "feature1_avg"],
                )
            ]
        )

    def test_feature_transform_with_window(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=SparkFunctionTransform(functions=["avg"],).with_(
                function=with_window,
                partition_by="id",
                order_by=TIMESTAMP_COLUMN,
                mode="row_windows",
                window_definition=["2 events", "3 events"],
            ),
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    df.columns,
                    [
                        "feature1",
                        "feature2",
                        "id",
                        TIMESTAMP_COLUMN,
                        "feature1_avg_over_2_events_row_windows",
                        "feature1_avg_over_3_events_row_windows",
                    ],
                )
            ]
        )

    def test_output_columns(self):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=SparkFunctionTransform(functions=["avg"],).with_(
                function=with_window,
                partition_by="id",
                order_by=TIMESTAMP_COLUMN,
                mode="fixed_windows",
                window_definition=["7 days", "2 weeks"],
            ),
        )

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(
                    df_columns,
                    [
                        "feature1_avg_over_7_days_fixed_windows",
                        "feature1_avg_over_2_weeks_fixed_windows",
                    ],
                )
            ]
        )

    def test_unsupported_function(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=SparkFunctionTransform(functions=["median"],),
            )

    def test_blank_function(self, feature_set_dataframe):
        with pytest.raises(ValueError, match="Functions must not be empty."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=SparkFunctionTransform(functions=[],),
            )

    def test_unsupported_windows(self, feature_set_dataframe):
        with pytest.raises(ValueError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=SparkFunctionTransform(functions=["avg"],).with_(
                    function=with_window,
                    partition_by="id",
                    order_by=TIMESTAMP_COLUMN,
                    mode="fixed_windows",
                    window_definition=["7 daily"],
                ),
            ).transform(feature_set_dataframe)

    def test_negative_windows(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=SparkFunctionTransform(functions=["avg"],).with_(
                    function=with_window,
                    partition_by="id",
                    order_by=TIMESTAMP_COLUMN,
                    mode="fixed_windows",
                    window_definition=["-2 weeks"],
                ),
            ).transform(feature_set_dataframe)

    def test_feature_transform_output_fixed_windows(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=SparkFunctionTransform(functions=["avg", "count"],).with_(
                function=with_window,
                partition_by="id",
                order_by=TIMESTAMP_COLUMN,
                mode="fixed_windows",
                window_definition=["2 minutes", "15 minutes"],
            ),
        )

        df = test_feature.transform(feature_set_dataframe).collect()

        assert df[0]["feature1_avg_over_2_minutes_fixed_windows"] == 200
        assert df[1]["feature1_avg_over_2_minutes_fixed_windows"] == 300
        assert df[2]["feature1_avg_over_2_minutes_fixed_windows"] == 400
        assert df[3]["feature1_avg_over_2_minutes_fixed_windows"] == 500
        assert df[0]["feature1_count_over_2_minutes_fixed_windows"] == 1
        assert df[1]["feature1_count_over_2_minutes_fixed_windows"] == 1
        assert df[2]["feature1_count_over_2_minutes_fixed_windows"] == 1
        assert df[3]["feature1_count_over_2_minutes_fixed_windows"] == 1
        assert df[0]["feature1_avg_over_15_minutes_fixed_windows"] == 200
        assert df[1]["feature1_avg_over_15_minutes_fixed_windows"] == 250
        assert df[2]["feature1_avg_over_15_minutes_fixed_windows"] == 350
        assert df[3]["feature1_avg_over_15_minutes_fixed_windows"] == 500
        assert df[0]["feature1_count_over_15_minutes_fixed_windows"] == 1
        assert df[1]["feature1_count_over_15_minutes_fixed_windows"] == 2
        assert df[2]["feature1_count_over_15_minutes_fixed_windows"] == 2
        assert df[3]["feature1_count_over_15_minutes_fixed_windows"] == 1

    def test_feature_transform_output_row_windows(
        self, feature_set_dataframe, target_df_rows_agg_2
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=SparkFunctionTransform(
                functions=["avg", "stddev_pop", "count"],
            ).with_(
                function=with_window,
                partition_by="id",
                order_by=TIMESTAMP_COLUMN,
                mode="row_windows",
                window_definition=["2 events"],
            ),
        )

        output_df = test_feature.transform(feature_set_dataframe)

        assert_dataframe_equality(output_df, target_df_rows_agg_2)
