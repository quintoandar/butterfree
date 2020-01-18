from butterfree.core.transform import Feature
from butterfree.core.transform.aggregation.aggregated_transform import Aggregation
import pytest


class TestAggregatedFeatureTransform:
    def test_feature_transform_no_alias(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            origin="mocked data",
            description="unit test feature with no alias",
        )

        test_feature.add(
            Aggregation(
                aggregations=["avg", "std"],
                partition="id",
                windows={"days": 7, "weeks": 2},
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
            origin="mocked data",
            description="unit test feature with no alias",
        )

        test_feature.add(
            Aggregation(
                aggregations=["avg", "std"], partition="id", windows={"days": 7, "weeks": 2}
            )
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    df.columns,
                    [
                        "new_feature",
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

    def test_undefined_aggregation(self, feature_set_dataframe):
        with pytest.raises(ValueError):
            test_feature = Feature(
                name="feature",
                alias="new_feature",
                origin="mocked data",
                description="unit test feature with no alias",
            )

            test_feature.add(
                Aggregation(
                    aggregations=["median"], partition="id",
                    windows={"days": 7, "weeks": 2}
                )
            )

            test_feature.transform(feature_set_dataframe)
