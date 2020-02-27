from unittest.mock import Mock

import pytest
from testing import compare_dataframes
from tests.unit.butterfree.core.transform.conftest import (
    feature_add,
    feature_divide,
    key_id,
    timestamp_c,
)

from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature


class TestFeatureSet:
    @pytest.mark.parametrize(
        "name, entity, description, keys, timestamp, features",
        [
            # invalid name
            (
                None,
                "entity",
                "description",
                [key_id],
                timestamp_c,
                [feature_add, feature_divide],
            ),
            # invalid entity
            (
                "name",
                None,
                "description",
                [key_id],
                timestamp_c,
                [feature_add, feature_divide],
            ),
            # invalid description
            (
                "name",
                "entity",
                None,
                [key_id],
                timestamp_c,
                [feature_add, feature_divide],
            ),
            # invalid keys
            (
                "name",
                "entity",
                "description",
                [None],
                timestamp_c,
                [feature_add, feature_divide],
            ),
            (
                "name",
                "entity",
                "description",
                [key_id],
                None,
                [feature_add, feature_divide],
            ),
            ("name", "entity", "description", [key_id], timestamp_c, [None],),
        ],
    )
    def test_cannot_instantiate(
        self, name, entity, description, keys, timestamp, features
    ):
        # act and assert
        with pytest.raises(ValueError):
            FeatureSet(name, entity, description, keys, timestamp, features)

    def test_getters(self, feature_add, feature_divide, key_id, timestamp_c):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"

        # act
        feature_set = FeatureSet(
            name,
            entity,
            description,
            [key_id],
            timestamp_c,
            [feature_add, feature_divide],
        )

        # assert
        assert name == feature_set.name
        assert entity == feature_set.entity
        assert description == feature_set.description
        assert [key_id] == feature_set.keys
        assert timestamp_c == feature_set.timestamp
        assert [feature_add, feature_divide] == feature_set.features
        assert "timestamp" == feature_set.timestamp_column
        assert ["id"] == feature_set.keys_columns

    def test_duplicate_keys(self, feature_add, feature_divide, key_id, timestamp_c):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"

        # act and assert
        with pytest.raises(KeyError):
            _ = FeatureSet(
                name,
                entity,
                description,
                [key_id, key_id],
                timestamp_c,
                [feature_add, feature_divide],
            )

    def test_duplicate_features(self, feature_add, key_id, timestamp_c):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"

        # act and assert
        with pytest.raises(KeyError):
            _ = FeatureSet(
                name,
                entity,
                description,
                [key_id],
                timestamp_c,
                [feature_add, feature_add],
            )

    def test_multiple_timestamps(self, feature_add, key_id, timestamp_c):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"
        timestamp_c.get_output_columns = Mock(return_value=["timestamp1", "timestamp2"])

        # act and assert
        with pytest.raises(ValueError):
            _ = FeatureSet(
                name, entity, description, [key_id], timestamp_c, [feature_add]
            )

    def test_columns(self, key_id, timestamp_c, feature_add, feature_divide):
        # arrange
        name = "name"
        entity = "entity"
        description = "description"

        # act
        fs = FeatureSet(
            name,
            entity,
            description,
            [key_id],
            timestamp_c,
            [feature_add, feature_divide],
        )
        out_columns = fs.columns

        # assert
        assert (
            out_columns
            == key_id.get_output_columns()
            + timestamp_c.get_output_columns()
            + feature_add.get_output_columns()
            + feature_divide.get_output_columns()
        )

    def test_construct(
        self, df, fs_target_df, key_id, timestamp_c, feature_add, feature_divide,
    ):
        # arrange
        feature_set = FeatureSet(
            "name",
            "entity",
            "description",
            [key_id],
            timestamp_c,
            [feature_add, feature_divide],
        )

        # act
        output_df = feature_set.construct(df)
        result_columns = output_df.columns

        # assert
        assert (
            result_columns
            == key_id.get_output_columns()
            + timestamp_c.get_output_columns()
            + feature_add.get_output_columns()
            + feature_divide.get_output_columns()
        )
        assert compare_dataframes(output_df, fs_target_df)
        assert output_df.is_cached

    def test_construct_invalid_df(
        self, key_id, timestamp_c, feature_add, feature_divide
    ):
        # arrange
        feature_set = FeatureSet(
            "name",
            "entity",
            "description",
            [key_id],
            timestamp_c,
            [feature_add, feature_divide],
        )

        # act and assert
        with pytest.raises(ValueError):
            _ = feature_set.construct("not a dataframe")

    def test_construct_transformations(
        self, df, fs_target_df, key_id, timestamp_c, feature_add, feature_divide,
    ):
        # arrange
        feature_set = FeatureSet(
            "name",
            "entity",
            "description",
            [key_id],
            timestamp_c,
            [feature_add, feature_divide],
        )

        # act
        output_df = feature_set.construct(df)

        # assert
        assert compare_dataframes(output_df, fs_target_df)

    def test_get_features_columns(self):
        # arrange
        feature_1 = Feature("feature1", "description")
        feature_1.get_output_columns = Mock(return_value=["col_a", "col_b"])

        feature_2 = Feature("feature2", "description")
        feature_2.get_output_columns = Mock(return_value=["col_c"])

        feature_3 = Feature("feature3", "description")
        feature_3.get_output_columns = Mock(return_value=["col_d"])

        target_features_columns = ["col_a", "col_b", "col_c", "col_d"]

        # act
        result_features_columns = FeatureSet._get_features_columns(
            feature_1, feature_2, feature_3
        )

        # assert
        assert target_features_columns == result_features_columns
