import pytest
from pyspark.sql import functions as F

from butterfree.core.transform import Feature
from butterfree.core.transform.custom.custom_transform import CustomTransform


def divide(df, name, column1, column2):

    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


class TestCustomTransform:
    def test_feature_transform_no_alias(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature", description="unit test feature with no alias",
        )

        test_feature.add(
            CustomTransform(
                transformer=divide,
                name=test_feature.name,
                column1="feature1",
                column2="feature2",
            )
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(df.columns, ["feature1", "feature2", "id", "feature"],)
            ]
        )

    def test_feature_transform_with_alias(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )

        test_feature.add(
            CustomTransform(
                transformer=divide,
                name=test_feature.name,
                column1="feature1",
                column2="feature2",
            )
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    df.columns, ["feature1", "feature2", "id", "new_feature"],
                )
            ]
        )

    def test_blank_transformer(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with no alias",
        )
        with pytest.raises(ValueError):
            test_feature.add(CustomTransform(transformer=[],))
            test_feature.transform(feature_set_dataframe)
