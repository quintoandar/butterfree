from butterfree.core.transform import Feature


class TestFeatureTransform:
    def test_feature_transform_no_alias(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature",
            origin="mocked data",
            description="unit test feature with no alias",
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, feature_set_dataframe.columns)])

    def test_feature_transform_with_alias(self, feature_set_dataframe):

        test_feature = Feature(
            name="feature",
            alias="new_feature",
            origin="mocked data",
            description="unit test feature with alias",
        )

        df = test_feature.transform(feature_set_dataframe)

        assert all([a == b for a, b in zip(df.columns, ["new_feature", "id", "ts"])])
