from unittest.mock import Mock

from butterfree.core.transform import Feature


class TestFeatureTransform:
    def test_args_without_transformation(self):

        test_feature = Feature(
            name="feature",
            alias="new_feature",
            origin="mocked data",
            description="unit test feature with alias",
            data_type="str",
        )

        assert test_feature.name[0] == "feature"
        assert test_feature.alias[0] == "new_feature"
        assert test_feature.origin[0] == "mocked data"
        assert test_feature.description[0] == "unit test feature with alias"
        assert test_feature.data_type[0] == "str"
        assert len(test_feature.transformations) == 0

    def test_args_with_transformation(self):

        test_feature = Feature(
            name="feature",
            alias="new_feature",
            origin="mocked data",
            description="unit test feature with alias",
            data_type="str",
        )
        component = Mock()
        test_feature.add(component)

        assert test_feature.name[0] == "feature"
        assert test_feature.alias[0] == "new_feature"
        assert test_feature.origin[0] == "mocked data"
        assert test_feature.description[0] == "unit test feature with alias"
        assert test_feature.data_type[0] == "str"
        assert len(test_feature.transformations) > 0

    def test_feature_transform_no_alias(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature",
            origin="mocked data",
            description="unit test feature with no alias",
            data_type="str",
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, feature_set_dataframe.columns)])

    def test_feature_transform_with_alias(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature",
            alias="new_feature",
            origin="mocked data",
            description="unit test feature with alias",
            data_type="str",
        )
        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, ["new_feature", "id", "ts"])])

    def test_feature_transform_with_transformation_no_alias(
        self, feature_set_dataframe
    ):

        test_feature = Feature(
            name="feature",
            origin="mocked data",
            description="unit test feature with alias",
            data_type="str",
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
            origin="mocked data",
            description="unit test feature with alias",
            data_type="str",
        )
        component = Mock()
        component.transform.return_value = feature_set_dataframe
        test_feature.add(component)

        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, ["new_feature", "id", "ts"])])
