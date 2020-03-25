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
            dtype=DataType.BIGINT,
            transformation=AggregatedTransform(
                functions=["avg", "stddev_pop"],
                group_by=["id", TIMESTAMP_COLUMN],
                column="feature1",
            ),
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    df.columns,
                    ["id", TIMESTAMP_COLUMN, "feature1__avg", "feature1__stddev_pop"],
                )
            ]
        )

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

    def test_feature_transform_output_rolling_windows(self, feature_set_dataframe):
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

    def test_feature_transform_output_row_windows_rows_agg(
        self, feature_set_dataframe, target_df_rows_agg_2
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg"],
                partition="id",
                windows=["2 events"],
                mode=["row_windows"],
            ),
        )

        output_df = test_feature.transform(feature_set_dataframe)

        assert_dataframe_equality(output_df, target_df_rows_agg_2)

    def test_feature_transform_rolling_windows_mode(
        self, mode_dataframe, mode_str_target_dataframe
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["mode"],
                partition="id",
                windows=["1 day"],
                mode=["rolling_windows"],
            ),
        )

        output_df = test_feature.transform(mode_dataframe)

        assert_dataframe_equality(output_df, mode_str_target_dataframe)

    def test_feature_transform_rolling_windows_mode_float(
        self, mode_dataframe, mode_num_target_dataframe
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            dtype=DataType.BIGINT,
            transformation=AggregatedTransform(
                aggregations=["mode"],
                partition="id",
                windows=["1 day"],
                mode=["rolling_windows"],
            ),
        )

        output_df = test_feature.transform(mode_dataframe)

        assert_dataframe_equality(output_df, mode_num_target_dataframe)
