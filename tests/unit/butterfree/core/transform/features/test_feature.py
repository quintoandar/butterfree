from unittest.mock import Mock

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.features import Feature


class TestFeature:
    def test_args_without_transformation(self):

        test_feature = Feature(
            name="feature", from_column="origin", description="unit test",
        )

        assert test_feature.name == "feature"
        assert test_feature.from_column == "origin"
        assert test_feature.description == "unit test"

    def test_args_with_transformation(self):

        test_feature = Feature(
            name="feature",
            from_column="origin",
            description="unit test",
            transformation=Mock(),
        )
        assert test_feature.name == "feature"
        assert test_feature.from_column == "origin"
        assert test_feature.description == "unit test"
        assert test_feature.transformation

    def test_feature_transform_no_from_column(self, target_df):

        test_feature = Feature(
            name="feature", description="unit test feature without transformation",
        )

        output_df = test_feature.transform(target_df)

        assert sorted(output_df.columns) == sorted(target_df.columns)

    def test_feature_transform_with_from_column(self, target_df):

        test_feature = Feature(
            name="new_feature1", from_column="feature1", description="unit test",
        )
        output_df = test_feature.transform(target_df)

        assert sorted(output_df.columns) == sorted(
            ["new_feature1", "feature2", "id", TIMESTAMP_COLUMN]
        )

    def test_feature_transform_with_dtype(self, target_df):

        test_feature = Feature(
            name="feature1", description="unit test", dtype=DataType.TIMESTAMP
        )
        output_df = test_feature.transform(target_df)

        assert dict(output_df.dtypes).get("feature1") == "timestamp"

    def test_feature_transform_with_transformation_no_from_column(self, target_df):
        some_transformation = Mock()
        some_transformation.transform.return_value = target_df

        test_feature = Feature(
            name="feature", description="unit test", transformation=some_transformation,
        )

        output_df = test_feature.transform(target_df)

        assert sorted(output_df.columns) == sorted(
            ["feature1", "feature2", "id", TIMESTAMP_COLUMN]
        )

    def test_feature_transform_with_transformation_and_alias(self, target_df):
        some_transformation = Mock()
        some_transformation.transform.return_value = target_df

        test_feature = Feature(
            name="feature1",
            from_column="origin",
            description="unit test",
            transformation=some_transformation,
        )

        output_df = test_feature.transform(target_df)

        assert sorted(output_df.columns) == sorted(
            ["feature1", "feature2", "id", TIMESTAMP_COLUMN]
        )

    def test_feature_get_output_columns_without_transformations(self):

        test_feature = Feature(
            name="feature", from_column="origin", description="unit test",
        )

        assert test_feature.get_output_columns() == [test_feature.name]

    def test_feature_get_output_columns_with_transformations(self, target_df):

        some_transformation = Mock()
        some_transformation.output_columns = target_df.columns

        test_feature = Feature(
            name="feature",
            from_column="origin",
            description="unit test",
            transformation=some_transformation,
        )

        assert test_feature.get_output_columns() == target_df.columns
