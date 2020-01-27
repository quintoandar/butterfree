import pytest

from butterfree.core.transform import Feature
from butterfree.core.transform.aggregation.aggregated_transform import (
    AggregatedTransform,
)


class TestAggregatedFeatureTransform:
    def test_feature_transform_no_alias(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature", description="unit test feature with no alias",
        )

        test_feature.add(
            AggregatedTransform(
                aggregations=["avg", "std"],
                partition="id",
                windows={"days": [7], "weeks": [2]},
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

    def test_feature_transform_with_alias(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )

        test_feature.add(
            AggregatedTransform(
                aggregations=["avg", "std"],
                partition="id",
                windows={"days": [7], "weeks": [2]},
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
                        "new_feature__avg_over_7_days",
                        "new_feature__avg_over_2_weeks",
                        "new_feature__std_over_7_days",
                        "new_feature__std_over_2_weeks",
                    ],
                )
            ]
        )

    def test_unsupported_aggregation(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )
        with pytest.raises(KeyError):
            test_feature.add(
                AggregatedTransform(
                    aggregations=["median"],
                    partition="id",
                    windows={"days": [7], "weeks": [2]},
                )
            )
            test_feature.transform(feature_set_dataframe)

    def test_blank_aggregation(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )
        with pytest.raises(ValueError, match="Aggregations must not be empty."):
            test_feature.add(
                AggregatedTransform(
                    aggregations=[],
                    partition="id",
                    windows={"days": [7], "weeks": [2]},
                )
            )
            test_feature.transform(feature_set_dataframe)

    def test_unsupported_window(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )
        with pytest.raises(KeyError):
            test_feature.add(
                AggregatedTransform(
                    aggregations=["avg", "std"],
                    partition="id",
                    windows={"daily": [7], "weeks": [2]},
                )
            )
            test_feature.transform(feature_set_dataframe)

    def test_blank_window(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )
        with pytest.raises(KeyError, match="Windows must not be empty."):
            test_feature.add(
                AggregatedTransform(
                    aggregations=["avg", "std"], partition="id", windows={},
                )
            )
            test_feature.transform(feature_set_dataframe)

    def test_int_window(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )
        with pytest.raises(KeyError, match="Windows must be a list."):
            test_feature.add(
                AggregatedTransform(
                    aggregations=["avg", "std"], partition="id", windows={"weeks": 2},
                )
            )
            test_feature.transform(feature_set_dataframe)

    def test_empty_window(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )
        with pytest.raises(KeyError, match="Windows must have one item at least."):
            test_feature.add(
                AggregatedTransform(
                    aggregations=["avg", "std"], partition="id", windows={"weeks": []},
                )
            )
            test_feature.transform(feature_set_dataframe)

    def test_negative_window(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )
        with pytest.raises(KeyError):
            test_feature.add(
                AggregatedTransform(
                    aggregations=["avg", "std"],
                    partition="id",
                    windows={"weeks": [-2]},
                )
            )
            test_feature.transform(feature_set_dataframe)

    def test_feature_transform_output(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature", description="unit test feature with no alias",
        )

        test_feature.add(
            AggregatedTransform(
                aggregations=["avg", "std"],
                partition="id",
                windows={"minutes": [2, 15]},
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
