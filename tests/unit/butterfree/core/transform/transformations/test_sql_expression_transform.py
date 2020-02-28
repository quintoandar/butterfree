import pytest
from testing import check_dataframe_equality

from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import SQLExpressionTransform


class TestSQLExpressionTransform:
    def test_feature_transform(self, target_df):
        test_feature = Feature(
            name="feature1_over_feature2",
            description="unit test",
            transformation=SQLExpressionTransform(expression="feature1/feature2"),
        )

        output_df = test_feature.transform(target_df)

        assert sorted(output_df.columns) == sorted(
            ["feature1", "feature2", "id", "timestamp", "feature1_over_feature2"]
        )

    def test_output_columns(self):
        test_feature = Feature(
            name="feature1_over_feature2",
            description="unit test",
            transformation=SQLExpressionTransform(expression="feature1/feature2"),
        )

        assert sorted(test_feature.get_output_columns()) == sorted(
            ["feature1_over_feature2"]
        )

    def test_feature_transform_output(self, target_df, sql_target_df):
        test_feature = Feature(
            name="feature1_over_feature2",
            description="unit test",
            transformation=SQLExpressionTransform(expression="feature1/feature2"),
        )

        output_df = test_feature.transform(target_df)

        assert check_dataframe_equality(output_df, sql_target_df)

    def test_feature_transform_invalid_output(self, target_df):
        with pytest.raises(Exception):
            test_feature = Feature(
                name="feature1_plus_a",
                description="unit test",
                transformation=SQLExpressionTransform(expression="feature2 + a"),
            )

            test_feature.transform(target_df)
