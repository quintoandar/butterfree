import pytest

from butterfree.constants import DataType
from butterfree.transform.features import Feature
from butterfree.transform.transformations import SQLExpressionTransform


class TestSQLExpressionTransform:
    def test_feature_transform(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature1_over_feature2",
            description="unit test",
            dtype=DataType.FLOAT,
            transformation=SQLExpressionTransform(expression="feature1/feature2"),
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
                        "timestamp",
                        "feature1_over_feature2",
                    ],
                )
            ]
        )

    def test_output_columns(self):
        test_feature = Feature(
            name="feature1_over_feature2",
            description="unit test",
            dtype=DataType.FLOAT,
            transformation=SQLExpressionTransform(expression="feature1/feature2"),
        )

        df_columns = test_feature.get_output_columns()

        assert all([a == b for a, b in zip(df_columns, ["feature1_over_feature2"],)])

    def test_feature_transform_output(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature1_over_feature2",
            description="unit test",
            dtype=DataType.FLOAT,
            transformation=SQLExpressionTransform(expression="feature1/feature2"),
        )

        df = test_feature.transform(feature_set_dataframe).collect()

        assert df[0]["feature1_over_feature2"] == 1
        assert df[1]["feature1_over_feature2"] == 1
        assert df[2]["feature1_over_feature2"] == 1
        assert df[3]["feature1_over_feature2"] == 1

    def test_feature_transform_invalid_output(self, feature_set_dataframe):
        with pytest.raises(Exception):
            test_feature = Feature(
                name="feature1_plus_a",
                description="unit test",
                dtype=DataType.FLOAT,
                transformation=SQLExpressionTransform(expression="feature2 + a"),
            )

            test_feature.transform(feature_set_dataframe).collect()
