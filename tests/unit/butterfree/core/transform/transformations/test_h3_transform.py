from unittest.mock import patch

import pytest
from testing import check_dataframe_equality

from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations.h3_transform import H3HashTransform


class TestH3Transform:
    def test_feature_transform(self, h3_df):
        test_feature = Feature(
            name="new_feature",
            description="unit test",
            transformation=H3HashTransform(
                h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
                lat_column="lat",
                lng_column="lng",
            ),
        )

        output_df = test_feature.transform(h3_df)

        assert sorted(output_df.columns) == sorted(
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

        assert sorted(test_feature.get_output_columns()) == sorted(
            [
                "lat_lng__h3_hash__6",
                "lat_lng__h3_hash__7",
                "lat_lng__h3_hash__8",
                "lat_lng__h3_hash__9",
                "lat_lng__h3_hash__10",
                "lat_lng__h3_hash__11",
                "lat_lng__h3_hash__12",
            ]
        )

    def test_feature_transform_output(self, h3_df, h3_target_df):
        test_feature = Feature(
            name="new_feature",
            description="unit test",
            transformation=H3HashTransform(
                h3_resolutions=[6, 7, 8], lat_column="lat", lng_column="lng",
            ),
        )

        output_df = test_feature.transform(h3_df)

        assert check_dataframe_equality(output_df, h3_target_df)

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
