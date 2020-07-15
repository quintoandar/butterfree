from unittest.mock import patch

import pytest

from butterfree.constants.data_type import DataType
from butterfree.testing.dataframe import assert_dataframe_equality
from butterfree.transform.features import Feature, KeyFeature
from butterfree.transform.transformations import H3HashTransform


class TestH3Transform:
    def test_feature_transform(self, h3_input_df, h3_target_df):
        # arrange
        test_feature = Feature(
            name="new_feature",
            description="unit test",
            dtype=DataType.STRING,
            transformation=H3HashTransform(
                h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
                lat_column="lat",
                lng_column="lng",
            ),
        )

        # act
        output_df = test_feature.transform(h3_input_df)

        # assert
        assert_dataframe_equality(output_df, h3_target_df)

    def test_output_columns(self):
        # arrange
        h3_feature = Feature(
            name="new_feature",
            description="unit test",
            dtype=DataType.STRING,
            transformation=H3HashTransform(
                h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
                lat_column="lat",
                lng_column="lng",
            ),
        )
        target_columns = [
            "lat_lng__h3_hash__6",
            "lat_lng__h3_hash__7",
            "lat_lng__h3_hash__8",
            "lat_lng__h3_hash__9",
            "lat_lng__h3_hash__10",
            "lat_lng__h3_hash__11",
            "lat_lng__h3_hash__12",
        ]

        # act
        output_columns = h3_feature.get_output_columns()

        # assert
        assert sorted(output_columns) == sorted(target_columns)

    def test_import_error(self):
        import sys

        with patch.dict(sys.modules, h3=None):
            modules = [m for m in sys.modules if m.startswith("butterfree")]
            for m in modules:
                del sys.modules[m]
            with pytest.raises(ModuleNotFoundError, match="you must install"):
                from butterfree.transform.transformations.h3_transform import (  # noqa
                    H3HashTransform,  # noqa
                )  # noqa

    def test_with_stack(self, h3_input_df, h3_with_stack_target_df):
        # arrange
        test_feature = KeyFeature(
            name="id",
            description="unit test",
            dtype=DataType.STRING,
            transformation=H3HashTransform(
                h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
                lat_column="lat",
                lng_column="lng",
            ).with_stack(),
        )

        # act
        output_df = test_feature.transform(h3_input_df)

        # assert
        assert_dataframe_equality(h3_with_stack_target_df, output_df)
