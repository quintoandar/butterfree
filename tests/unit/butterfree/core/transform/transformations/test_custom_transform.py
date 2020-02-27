import pytest
from pyspark.sql import functions as F
from testing import compare_dataframes

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import CustomTransform


def divide(df, parent_feature, column1, column2):
    name = parent_feature.get_output_columns()[0]
    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


class TestCustomTransform:
    def test_feature_transform(self, target_df):

        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=CustomTransform(
                transformer=divide, column1="feature1", column2="feature2",
            ),
        )

        output_df = test_feature.transform(target_df)

        assert sorted(output_df.columns) == sorted(
            ["feature1", "feature2", "id", TIMESTAMP_COLUMN, "feature"]
        )

    def test_output_columns(self):

        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=CustomTransform(
                transformer=divide, column1="feature1", column2="feature2",
            ),
        )

        output_df_columns = test_feature.get_output_columns()

        assert isinstance(output_df_columns, list)
        assert output_df_columns == ["feature"]

    def test_custom_transform_output(self, custom_target_df):
        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=CustomTransform(
                transformer=divide, column1="feature1", column2="feature2",
            ),
        )

        output_df = test_feature.transform(custom_target_df)

        assert compare_dataframes(output_df, custom_target_df)

    def test_blank_transformer(self):
        with pytest.raises(ValueError):
            Feature(
                name="feature",
                description="unit test",
                transformation=CustomTransform(transformer=None),
            )
