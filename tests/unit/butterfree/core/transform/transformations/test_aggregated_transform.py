import pytest
from testing import compare_dataframes

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import AggregatedTransform


class TestAggregatedTransform:
    def test_feature_transform(self, target_df):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "stddev_pop"],
                partition="id",
                windows=["7 days", "2 weeks"],
                mode=["fixed_windows"],
            ),
        )

        output_df = test_feature.transform(target_df)

        assert sorted(output_df.columns) == sorted(
            [
                "feature1",
                "feature2",
                "id",
                TIMESTAMP_COLUMN,
                "feature1__avg_over_7_days_fixed_windows",
                "feature1__avg_over_2_weeks_fixed_windows",
                "feature1__stddev_pop_over_7_days_fixed_windows",
                "feature1__stddev_pop_over_2_weeks_fixed_windows",
            ]
        )

    def test_output_columns(self):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "stddev_pop"],
                partition="id",
                windows=["7 days", "2 weeks"],
                mode=["fixed_windows"],
            ),
        )

        assert sorted(test_feature.get_output_columns()) == sorted(
            [
                "feature1__avg_over_7_days_fixed_windows",
                "feature1__avg_over_2_weeks_fixed_windows",
                "feature1__stddev_pop_over_7_days_fixed_windows",
                "feature1__stddev_pop_over_2_weeks_fixed_windows",
            ]
        )

    def test_unsupported_aggregation(self):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["median"],
                    partition="id",
                    windows=["7 days", "2 weeks"],
                    mode=["fixed_windows"],
                ),
            )

    def test_blank_aggregation(self):
        with pytest.raises(ValueError, match="Aggregations must not be empty."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=[],
                    partition="id",
                    windows=["7 days", "2 weeks"],
                    mode=["fixed_windows"],
                ),
            )

    def test_unsupported_window(self):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "stddev_pop"],
                    partition="id",
                    windows=["7 daily", "2 weeks"],
                    mode=["fixed_windows"],
                ),
            )

    def test_blank_window(self):
        with pytest.raises(KeyError, match="Windows must not be empty."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "stddev_pop"],
                    partition="id",
                    windows=[],
                    mode=["fixed_windows"],
                ),
            )

    def test_int_window(self):
        with pytest.raises(KeyError, match="Windows must be a list."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "stddev_pop"],
                    partition="id",
                    windows={"2 weeks"},
                    mode=["fixed_windows"],
                ),
            )

    def test_negative_window(self):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "stddev_pop"],
                    partition="id",
                    windows=["-2 weeks"],
                    mode=["fixed_windows"],
                ),
            )

    def test_feature_transform_output_fixed_windows(
        self, target_df, fixed_windows_target_df
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "stddev_pop", "count"],
                partition="id",
                windows=["2 minutes", "15 minutes"],
                mode=["fixed_windows"],
            ),
        )

        output_df = test_feature.transform(target_df)

        assert compare_dataframes(output_df, fixed_windows_target_df)

    def test_feature_transform_output_rolling_windows(
        self, target_df, rolling_windows_target_df
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "stddev_pop", "count"],
                partition="id",
                windows=["1 day", "3 days"],
                mode=["rolling_windows"],
            ),
        )

        output_df = test_feature.transform(target_df)

        assert compare_dataframes(output_df, rolling_windows_target_df)

    def test_feature_transform_empty_mode(self):
        with pytest.raises(ValueError, match="Modes must not be empty."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "stddev_pop"],
                    partition="id",
                    windows=["25 minutes", "15 minutes"],
                    mode=[],
                ),
            )

    def test_feature_transform_two_modes(self):
        with pytest.raises(
            NotImplementedError, match="We currently accept just one mode per feature."
        ):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "stddev_pop"],
                    partition="id",
                    windows=["25 minutes", "15 minutes"],
                    mode=["fixed_windows", "rolling_windows"],
                ),
            )

    def test_feature_transform_invalid_mode(self):
        with pytest.raises(KeyError, match="rolling_stones is not supported."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "stddev_pop"],
                    partition="id",
                    windows=["25 minutes", "15 minutes"],
                    mode=["rolling_stones"],
                ),
            )

    def test_feature_transform_invalid_rolling_window(self):
        with pytest.raises(
            ValueError, match="Window duration has to be greater or equal than 1 day"
        ):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "stddev_pop"],
                    partition="id",
                    windows=["25 minutes", "15 minutes"],
                    mode=["rolling_windows"],
                ),
            )
