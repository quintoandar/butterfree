import pytest

from butterfree.core.transform import Feature
from butterfree.core.transform.aggregation.aggregated_transform import (
    AggregatedTransform,
)


class TestAggregatedTransform:
    def test_feature_transform_no_alias(self, feature_set_dataframe):
class TestAggregatedTransform:
    def test_feature_transform(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "std"],
                partition="id",
                windows=["7 days", "2 weeks"],
            )
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    df.columns,
                    [
                        "feature",
                        "id",
                        "ts",
                        "timestamp",
                        "feature__avg_over_7_days",
                        "feature__avg_over_2_weeks",
                        "feature__std_over_7_days",
                        "feature__std_over_2_weeks",
                    ],
                )
            ]
        )

    def test_output_columns(self):
        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "std"],
                partition="id",
                windows=["7 days", "2 weeks"],
            )
        )

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(
                    df_columns,
                    [
                        "feature__avg_over_7_days",
                        "feature__avg_over_2_weeks",
                        "feature__std_over_7_days",
                        "feature__std_over_2_weeks",
                    ],
                )
            ]
        )

    def test_unsupported_aggregation(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["median"],
                    partition="id",
                    windows={"days": [7], "weeks": [2]},
                ),
            )

    def test_blank_aggregation(self, feature_set_dataframe):
        with pytest.raises(ValueError, match="Aggregations must not be empty."):
            Feature(
                name="feature",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=[],
                    partition="id",
                    windows={"days": [7], "weeks": [2]},
                ),
            )

    def test_unsupported_window(self, feature_set_dataframe):
        with pytest.raises(ValueError, match="Aggregations must not be empty."):
            Feature(
                name="feature",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=[],
                    partition="id",
                    windows={"daily": [7], "weeks": [2]},
                ),
            )

    def test_blank_window(self, feature_set_dataframe):
        with pytest.raises(KeyError, match="Windows must not be empty."):
            Feature(
                name="feature",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "std"], partition="id", windows={},
                ),
            )

    def test_int_window(self, feature_set_dataframe):
        with pytest.raises(KeyError, match="Windows must be a list."):
            Feature(
                name="feature",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "std"], partition="id", windows={"weeks": 2},
                ),
            )

    def test_empty_window(self, feature_set_dataframe):
        with pytest.raises(KeyError, match="Windows must have one item at least."):
            Feature(
                name="feature",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "std"], partition="id", windows={"weeks": []},
                ),
            )

    def test_negative_window(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature",
                description="unit test",
                transformation=AggregatedTransform(
                    aggregations=["avg", "std"],
                    partition="id",
                    windows={"weeks": [-2]},
                ),
            )

    def test_feature_transform_output(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=AggregatedTransform(
                aggregations=["avg", "std"],
                partition="id",
                windows=["2 minutes", "15 minutes"],
            )
        )

        df = test_feature.transform(feature_set_dataframe).collect()

        assert df[0]["feature__avg_over_2_minutes"] == 200
        assert df[1]["feature__avg_over_2_minutes"] == 300
        assert df[2]["feature__avg_over_2_minutes"] == 400
        assert df[3]["feature__avg_over_2_minutes"] == 500
        assert df[0]["feature__std_over_2_minutes"] == 0
        assert df[1]["feature__std_over_2_minutes"] == 0
        assert df[2]["feature__std_over_2_minutes"] == 0
        assert df[3]["feature__std_over_2_minutes"] == 0
        assert df[0]["feature__avg_over_15_minutes"] == 200
        assert df[1]["feature__avg_over_15_minutes"] == 250
        assert df[2]["feature__avg_over_15_minutes"] == 350
        assert df[3]["feature__avg_over_15_minutes"] == 500
        assert df[0]["feature__std_over_15_minutes"] == 0
        assert df[1]["feature__std_over_15_minutes"] == 50
        assert df[2]["feature__std_over_15_minutes"] == 50
        assert df[3]["feature__std_over_15_minutes"] == 0
