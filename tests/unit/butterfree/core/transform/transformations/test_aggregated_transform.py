import json

import pytest

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import AggregatedTransform
from butterfree.testing.dataframe import assert_dataframe_equality


class TestAggregatedTransform:
    def test_feature_transform(self, feature_set_dataframe):
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
                        "origin_ts",
                        TIMESTAMP_COLUMN,
                        "feature1__avg_over_7_days_fixed_windows",
                        "feature1__avg_over_2_weeks_fixed_windows",
                        "feature1__stddev_pop_over_7_days_fixed_windows",
                        "feature1__stddev_pop_over_2_weeks_fixed_windows",
                    ],
                )
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

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(
                    df_columns,
                    [
                        "feature1__avg_over_7_days_fixed_windows",
                        "feature1__avg_over_2_weeks_fixed_windows",
                        "feature1__stddev_pop_over_7_days_fixed_windows",
                        "feature1__stddev_pop_over_2_weeks_fixed_windows",
                    ],
                )
            ]
        )

    def test_unsupported_aggregation(self, feature_set_dataframe):
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

    def test_blank_aggregation(self, feature_set_dataframe):
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

    def test_unsupported_window(self, feature_set_dataframe):
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

    def test_blank_window(self, feature_set_dataframe):
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

    def test_int_window(self, feature_set_dataframe):
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

    def test_negative_window(self, feature_set_dataframe):
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

    def test_feature_transform_output_fixed_windows(self, feature_set_dataframe):
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

        df = test_feature.transform(feature_set_dataframe).collect()

        assert df[0]["feature1__avg_over_2_minutes_fixed_windows"] == 200
        assert df[1]["feature1__avg_over_2_minutes_fixed_windows"] == 300
        assert df[2]["feature1__avg_over_2_minutes_fixed_windows"] == 400
        assert df[3]["feature1__avg_over_2_minutes_fixed_windows"] == 500
        assert df[0]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[1]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[2]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[3]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[0]["feature1__count_over_2_minutes_fixed_windows"] == 1
        assert df[1]["feature1__count_over_2_minutes_fixed_windows"] == 1
        assert df[2]["feature1__count_over_2_minutes_fixed_windows"] == 1
        assert df[3]["feature1__count_over_2_minutes_fixed_windows"] == 1
        assert df[0]["feature1__avg_over_15_minutes_fixed_windows"] == 200
        assert df[1]["feature1__avg_over_15_minutes_fixed_windows"] == 250
        assert df[2]["feature1__avg_over_15_minutes_fixed_windows"] == 350
        assert df[3]["feature1__avg_over_15_minutes_fixed_windows"] == 500
        assert df[0]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 0
        assert df[1]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 50
        assert df[2]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 50
        assert df[3]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 0
        assert df[0]["feature1__count_over_15_minutes_fixed_windows"] == 1
        assert df[1]["feature1__count_over_15_minutes_fixed_windows"] == 2
        assert df[2]["feature1__count_over_15_minutes_fixed_windows"] == 2
        assert df[3]["feature1__count_over_15_minutes_fixed_windows"] == 1

    def test_feature_transform_output_rolling_windows(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "stddev_pop", "count"],
                partition="id",
                windows=["1 day", "1 week"],
                mode=["rolling_windows"],
            ),
        )

        df = (
            test_feature.transform(feature_set_dataframe).orderBy("timestamp").collect()
        )

        assert df[0]["feature1__avg_over_1_day_rolling_windows"] == 350
        assert df[1]["feature1__avg_over_1_day_rolling_windows"] is None
        assert df[2]["feature1__avg_over_1_day_rolling_windows"] is None
        assert df[3]["feature1__avg_over_1_day_rolling_windows"] is None
        assert df[4]["feature1__avg_over_1_day_rolling_windows"] is None
        assert df[5]["feature1__avg_over_1_day_rolling_windows"] is None
        assert df[6]["feature1__avg_over_1_day_rolling_windows"] is None
        assert (
            df[0]["feature1__stddev_pop_over_1_day_rolling_windows"]
            == 111.80339887498948
        )
        assert df[1]["feature1__stddev_pop_over_1_day_rolling_windows"] is None
        assert df[2]["feature1__stddev_pop_over_1_day_rolling_windows"] is None
        assert df[3]["feature1__stddev_pop_over_1_day_rolling_windows"] is None
        assert df[4]["feature1__stddev_pop_over_1_day_rolling_windows"] is None
        assert df[5]["feature1__stddev_pop_over_1_day_rolling_windows"] is None
        assert df[6]["feature1__stddev_pop_over_1_day_rolling_windows"] is None
        assert df[0]["feature1__count_over_1_day_rolling_windows"] == 4
        assert df[1]["feature1__count_over_1_day_rolling_windows"] is None
        assert df[2]["feature1__count_over_1_day_rolling_windows"] is None
        assert df[3]["feature1__count_over_1_day_rolling_windows"] is None
        assert df[4]["feature1__count_over_1_day_rolling_windows"] is None
        assert df[5]["feature1__count_over_1_day_rolling_windows"] is None
        assert df[6]["feature1__count_over_1_day_rolling_windows"] is None
        assert df[0]["feature1__avg_over_1_week_rolling_windows"] == 350
        assert df[1]["feature1__avg_over_1_week_rolling_windows"] == 350
        assert df[2]["feature1__avg_over_1_week_rolling_windows"] == 350
        assert df[3]["feature1__avg_over_1_week_rolling_windows"] == 350
        assert df[4]["feature1__avg_over_1_week_rolling_windows"] == 350
        assert df[5]["feature1__avg_over_1_week_rolling_windows"] == 350
        assert df[6]["feature1__avg_over_1_week_rolling_windows"] == 350
        assert (
            df[0]["feature1__stddev_pop_over_1_week_rolling_windows"]
            == 111.80339887498948
        )
        assert (
            df[1]["feature1__stddev_pop_over_1_week_rolling_windows"]
            == 111.80339887498948
        )
        assert (
            df[2]["feature1__stddev_pop_over_1_week_rolling_windows"]
            == 111.80339887498948
        )
        assert (
            df[3]["feature1__stddev_pop_over_1_week_rolling_windows"]
            == 111.80339887498948
        )
        assert (
            df[4]["feature1__stddev_pop_over_1_week_rolling_windows"]
            == 111.80339887498948
        )
        assert (
            df[5]["feature1__stddev_pop_over_1_week_rolling_windows"]
            == 111.80339887498948
        )
        assert (
            df[6]["feature1__stddev_pop_over_1_week_rolling_windows"]
            == 111.80339887498948
        )
        assert df[0]["feature1__count_over_1_week_rolling_windows"] == 4
        assert df[1]["feature1__count_over_1_week_rolling_windows"] == 4
        assert df[2]["feature1__count_over_1_week_rolling_windows"] == 4
        assert df[3]["feature1__count_over_1_week_rolling_windows"] == 4
        assert df[4]["feature1__count_over_1_week_rolling_windows"] == 4
        assert df[5]["feature1__count_over_1_week_rolling_windows"] == 4
        assert df[6]["feature1__count_over_1_week_rolling_windows"] == 4

    def test_feature_transform_empty_mode(self, feature_set_dataframe):
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

    def test_feature_transform_two_modes(self, feature_set_dataframe):
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

    def test_feature_transform_invalid_mode(self, feature_set_dataframe):
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

    def test_feature_transform_invalid_rolling_window(self, feature_set_dataframe):
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

    def test_feature_transform_collect_set_fixed_windows(
        self, with_house_ids_dataframe, spark_context, spark_session
    ):
        # given
        data = [
            {
                "user_id": 1,
                "timestamp": "2016-04-12 00:00:00",
                "house_id__collect_set_over_1_day_rolling_windows": [123, 400],
            },
            {
                "user_id": 1,
                "timestamp": "2016-04-13 00:00:00",
                "house_id__collect_set_over_1_day_rolling_windows": [192],
            },
            {
                "user_id": 1,
                "timestamp": "2016-04-16 00:00:00",
                "house_id__collect_set_over_1_day_rolling_windows": [715],
            },
        ]
        expected_df = spark_session.read.json(
            spark_context.parallelize(data).map(lambda x: json.dumps(x))
        )
        expected_df = expected_df.withColumn(
            TIMESTAMP_COLUMN, expected_df.timestamp.cast(DataType.TIMESTAMP.value)
        )

        # when
        test_feature = Feature(
            name="house_id",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["collect_set"],
                partition="user_id",
                windows=["1 day"],
                mode=["rolling_windows"],
            ),
        )

        df = test_feature.transform(with_house_ids_dataframe)

        # then
        assert_dataframe_equality(df, expected_df)
