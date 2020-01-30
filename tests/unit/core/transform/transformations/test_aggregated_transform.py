import pytest

from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import AggregatedTransform


class TestAggregatedTransform:
    def test_feature_transform(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "std"],
                partition="id",
                windows=["7 days", "2 weeks"],
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
                        "ts",
                        "timestamp",
                        "feature1__avg_over_7_days",
                        "feature1__avg_over_2_weeks",
                        "feature1__std_over_7_days",
                        "feature1__std_over_2_weeks",
                    ],
                )
            ]
        )

    def test_output_columns(self):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "std"],
                partition="id",
                windows=["7 days", "2 weeks"],
            ),
        )

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(
                    df_columns,
                    [
                        "feature1__avg_over_7_days",
                        "feature1__avg_over_2_weeks",
                        "feature1__std_over_7_days",
                        "feature1__std_over_2_weeks",
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
                ),
            )

    def test_blank_aggregation(self, feature_set_dataframe):
        with pytest.raises(ValueError, match="Aggregations must not be empty."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=[], partition="id", windows=["7 days", "2 weeks"],
                ),
            )

    def test_unsupported_window(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "std"],
                    partition="id",
                    windows=["7 daily", "2 weeks"],
                ),
            )

    def test_blank_window(self, feature_set_dataframe):
        with pytest.raises(KeyError, match="Windows must not be empty."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "std"], partition="id", windows=[],
                ),
            )

    def test_int_window(self, feature_set_dataframe):
        with pytest.raises(KeyError, match="Windows must be a list."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "std"], partition="id", windows={"2 weeks"},
                ),
            )

    def test_negative_window(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "std"], partition="id", windows=["-2 weeks"],
                ),
            )

    def test_feature_transform_output(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "std"],
                partition="id",
                windows=["2 minutes", "15 minutes"],
            ),
        )

        df = test_feature.transform(feature_set_dataframe).collect()

        assert df[0]["feature1__avg_over_2_minutes"] == 200
        assert df[1]["feature1__avg_over_2_minutes"] == 300
        assert df[2]["feature1__avg_over_2_minutes"] == 400
        assert df[3]["feature1__avg_over_2_minutes"] == 500
        assert df[0]["feature1__std_over_2_minutes"] == 0
        assert df[1]["feature1__std_over_2_minutes"] == 0
        assert df[2]["feature1__std_over_2_minutes"] == 0
        assert df[3]["feature1__std_over_2_minutes"] == 0
        assert df[0]["feature1__avg_over_15_minutes"] == 200
        assert df[1]["feature1__avg_over_15_minutes"] == 250
        assert df[2]["feature1__avg_over_15_minutes"] == 350
        assert df[3]["feature1__avg_over_15_minutes"] == 500
        assert df[0]["feature1__std_over_15_minutes"] == 0
        assert df[1]["feature1__std_over_15_minutes"] == 50
        assert df[2]["feature1__std_over_15_minutes"] == 50
        assert df[3]["feature1__std_over_15_minutes"] == 0
