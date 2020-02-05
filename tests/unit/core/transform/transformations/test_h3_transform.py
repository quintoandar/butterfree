from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import H3Transform


class TestH3Transform:
    def test_feature_transform(self, h3_dataframe):
        test_feature = Feature(
            name="new_feature",
            description="unit test",
            transformation=H3Transform(lat_column="lat", lng_column="lng"),
        )

        df = test_feature.transform(h3_dataframe)

        assert all(
            [
                a == b
                for a, b in zip(
                    df.columns,
                    [
                        "feature",
                        "id",
                        "lat",
                        "lng",
                        "new_feature__H6",
                        "new_feature__H7",
                        "new_feature__H8",
                        "new_feature__H9",
                        "new_feature__H10",
                        "new_feature__H11",
                        "new_feature__H12",
                    ],
                )
            ]
        )

    def test_output_columns(self):
        test_feature = Feature(
            name="new_feature",
            description="unit test",
            transformation=H3Transform(lat_column="lat", lng_column="lng"),
        )

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(
                    df_columns,
                    [
                        "new_feature__H6",
                        "new_feature__H7",
                        "new_feature__H8",
                        "new_feature__H9",
                        "new_feature__H10",
                        "new_feature__H11",
                        "new_feature__H12",
                    ],
                )
            ]
        )

    def test_feature_transform_output(self, h3_dataframe):
        test_feature = Feature(
            name="new_feature",
            description="unit test",
            transformation=H3Transform(lat_column="lat", lng_column="lng"),
        )

        df = test_feature.transform(h3_dataframe).collect()

        for line in range(0, 4):
            assert df[line]["new_feature__H6"] == "86a8100efffffff"
            assert df[line]["new_feature__H7"] == "87a8100eaffffff"
            assert df[line]["new_feature__H8"] == "88a8100ea1fffff"
            assert df[line]["new_feature__H9"] == "89a8100ea0fffff"
            assert df[line]["new_feature__H10"] == "8aa8100ea0d7fff"
            assert df[line]["new_feature__H11"] == "8ba8100ea0d5fff"
            assert df[line]["new_feature__H12"] == "8ca8100ea0d57ff"
