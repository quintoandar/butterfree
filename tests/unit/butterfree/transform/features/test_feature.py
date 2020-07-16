from unittest.mock import Mock

from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.testing.dataframe import assert_column_equality
from butterfree.transform.features import Feature


class TestFeature:
    def test_args_without_transformation(self):

        test_feature = Feature(
            name="feature",
            from_column="origin",
            description="unit test",
            dtype=DataType.BIGINT,
        )

        assert test_feature.name == "feature"
        assert test_feature.from_column == "origin"
        assert test_feature.description == "unit test"
        assert test_feature.dtype == DataType.BIGINT

    def test_args_with_transformation(self):

        test_feature = Feature(
            name="feature",
            from_column="origin",
            description="unit test",
            dtype=DataType.BIGINT,
            transformation=Mock(),
        )
        assert test_feature.name == "feature"
        assert test_feature.from_column == "origin"
        assert test_feature.description == "unit test"
        assert test_feature.dtype == DataType.BIGINT
        assert test_feature.transformation

    def test_feature_transform_no_from_column(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature",
            description="unit test feature without transformation",
            dtype=DataType.BIGINT,
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, feature_set_dataframe.columns)])

    def test_feature_transform_with_from_column(self, feature_set_dataframe):

        test_feature = Feature(
            name="new_feature",
            from_column="feature",
            description="unit test",
            dtype=DataType.BIGINT,
        )
        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    sorted(df.columns),
                    sorted(["new_feature", "id", TIMESTAMP_COLUMN, "feature"]),
                )
            ]
        )

    def test_feature_transform_with_from_column_and_column_name_exists(
        self, feature_set_dataframe
    ):

        test_feature = Feature(
            name="feature",
            from_column="id",
            description="unit test",
            dtype=DataType.BIGINT,
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    sorted(df.columns), sorted(["id", TIMESTAMP_COLUMN, "feature"])
                )
            ]
        )

        assert_column_equality(df, feature_set_dataframe, "feature", "id")

    def test_feature_transform_with_dtype(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature", description="unit test", dtype=DataType.TIMESTAMP,
        )
        df = test_feature.transform(feature_set_dataframe)

        assert dict(df.dtypes).get("feature") == "timestamp"

    def test_feature_transform_with_transformation_no_from_column(
        self, feature_set_dataframe
    ):
        some_transformation = Mock()
        some_transformation.transform.return_value = feature_set_dataframe

        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=some_transformation,
            dtype=DataType.BIGINT,
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    sorted(df.columns), sorted(["feature", "id", TIMESTAMP_COLUMN])
                )
            ]
        )

    def test_feature_transform_with_transformation_and_alias(
        self, feature_set_dataframe
    ):
        some_transformation = Mock()
        some_transformation.transform.return_value = feature_set_dataframe

        test_feature = Feature(
            name="feature",
            from_column="origin",
            description="unit test",
            transformation=some_transformation,
            dtype=DataType.BIGINT,
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    sorted(df.columns), sorted(["feature", "id", TIMESTAMP_COLUMN])
                )
            ]
        )

    def test_feature_get_output_columns_without_transformations(self):

        test_feature = Feature(
            name="feature",
            from_column="origin",
            description="unit test",
            dtype=DataType.BIGINT,
        )

        assert test_feature.get_output_columns() == [test_feature.name]

    def test_feature_get_output_columns_with_transformations(
        self, feature_set_dataframe
    ):

        some_transformation = Mock()
        some_transformation.output_columns = feature_set_dataframe.columns

        test_feature = Feature(
            name="feature",
            from_column="origin",
            description="unit test",
            transformation=some_transformation,
            dtype=DataType.BIGINT,
        )

        assert test_feature.get_output_columns() == feature_set_dataframe.columns
