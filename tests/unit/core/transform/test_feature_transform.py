from unittest.mock import Mock

from butterfree.core.transform import Feature


class TestFeatureTransform:
    def test_args_without_transformation(self):

        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with alias",
        )

        assert test_feature.name == "feature"
        assert test_feature.alias == "new_feature"
        assert test_feature.description == "unit test feature with alias"
        assert not test_feature.transformations

    def test_args_with_transformation(self):

        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with alias",
        )
        component = Mock()
        test_feature.add(component)

        assert test_feature.name == "feature"
        assert test_feature.alias == "new_feature"
        assert test_feature.description == "unit test feature with alias"
        assert test_feature.transformations

    def test_feature_transform_no_alias(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature", description="unit test feature with no alias",
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, feature_set_dataframe.columns)])

    def test_feature_transform_with_alias(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with alias",
        )
        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, ["new_feature", "id", "ts"])])

    def test_feature_transform_with_transformation_no_alias(
        self, feature_set_dataframe
    ):

        test_feature = Feature(
            name="feature", description="unit test feature with alias",
        )
        component = Mock()
        component.transform.return_value = feature_set_dataframe
        test_feature.add(component)

        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, ["feature", "id", "ts"])])

    def test_feature_transform_with_transformation_and_alias(
        self, feature_set_dataframe
    ):

        test_feature = Feature(
            name="feature",
            alias="new_feature",
            description="unit test feature with alias",
        )
        component = Mock()
        component.transform.return_value = feature_set_dataframe
        test_feature.add(component)

        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, ["feature", "id", "ts"])])
