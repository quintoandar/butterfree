import pytest
from pyspark.sql import functions as F

from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import CustomTransform


def divide(df, fs, column1, column2):
    name = fs.get_output_columns()[0]
    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


class TestCustomTransform:
    def test_feature_transform(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=CustomTransform(
                transformer=divide, column1="feature1", column2="feature2",
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
                        "feature",
                    ],
                )
            ]
        )

    def test_output_columns(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=CustomTransform(
                transformer=divide, column1="feature1", column2="feature2",
            ),
        )

        df_columns = test_feature.get_output_columns()

        assert isinstance(df_columns, list)
        assert df_columns == ["feature"]

    def test_custom_transform_output(self, feature_set_dataframe):
        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=CustomTransform(
                transformer=divide, column1="feature1", column2="feature2",
            ),
        )

        df = test_feature.transform(feature_set_dataframe).collect()

        assert df[0]["feature"] == 1
        assert df[1]["feature"] == 1
        assert df[2]["feature"] == 1
        assert df[3]["feature"] == 1

    def test_blank_transformer(self, feature_set_dataframe):
        with pytest.raises(ValueError):
            Feature(
                name="feature",
                description="unit test",
                transformation=CustomTransform(transformer=None),
            )
