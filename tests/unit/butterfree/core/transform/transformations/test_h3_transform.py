from unittest.mock import patch

import pytest

from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations.h3_transform import H3HashTransform


class TestH3Transform:
    def test_feature_transform(self, h3_dataframe):
        test_feature = Feature(
            name="new_feature",
            description="unit test",
            transformation=H3HashTransform(
                h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
                lat_column="lat",
                lng_column="lng",
            ),
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
                        "lat_lng__h3_hash__6",
                        "lat_lng__h3_hash__7",
                        "lat_lng__h3_hash__8",
                        "lat_lng__h3_hash__9",
                        "lat_lng__h3_hash__10",
                        "lat_lng__h3_hash__11",
                        "lat_lng__h3_hash__12",
                    ],
                )
            ]
        )

    def test_output_columns(self):
        test_feature = Feature(
            name="new_feature",
            description="unit test",
            transformation=H3HashTransform(
                h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
                lat_column="lat",
                lng_column="lng",
            ),
        )

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(
                    df_columns,
                    [
                        "lat_lng__h3_hash__6",
                        "lat_lng__h3_hash__7",
                        "lat_lng__h3_hash__8",
                        "lat_lng__h3_hash__9",
                        "lat_lng__h3_hash__10",
                        "lat_lng__h3_hash__11",
                        "lat_lng__h3_hash__12",
                    ],
                )
            ]
        )

    def test_feature_transform_output(self, h3_dataframe):
        test_feature = Feature(
            name="new_feature",
            description="unit test",
            transformation=H3HashTransform(
                h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
                lat_column="lat",
                lng_column="lng",
            ),
        )

        df = test_feature.transform(h3_dataframe).collect()

        for line in range(0, 4):
            assert df[line]["lat_lng__h3_hash__6"] == "86a8100efffffff"
            assert df[line]["lat_lng__h3_hash__7"] == "87a8100eaffffff"
            assert df[line]["lat_lng__h3_hash__8"] == "88a8100ea1fffff"
            assert df[line]["lat_lng__h3_hash__9"] == "89a8100ea0fffff"
            assert df[line]["lat_lng__h3_hash__10"] == "8aa8100ea0d7fff"
            assert df[line]["lat_lng__h3_hash__11"] == "8ba8100ea0d5fff"
            assert df[line]["lat_lng__h3_hash__12"] == "8ca8100ea0d57ff"

    def test_import_error(self):
        import sys

        with patch.dict(sys.modules, h3=None):
            modules = [m for m in sys.modules if m.startswith("butterfree")]
            for m in modules:
                del sys.modules[m]
            with pytest.raises(ModuleNotFoundError, match="you must install"):
                from butterfree.core.transform.transformations.h3_transform import (  # noqa
                    H3HashTransform,  # noqa
                )  # noqa
