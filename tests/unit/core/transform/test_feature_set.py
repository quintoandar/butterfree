from unittest.mock import Mock

import pytest
from tests.unit.core.transform.conftest import (
    feature_add,
    feature_divide,
    key_id,
    timestamp_c,
)

from butterfree.core.transform import FeatureSet


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
        self,
        dataframe,
        feature_set_dataframe,
        key_id,
        timestamp_c,
        feature_add,
        feature_divide,
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
        result_df = feature_set.construct(dataframe)
        result_columns = result_df.columns

        # assert
        assert (
            result_columns
            == key_id.get_output_columns()
            + timestamp_c.get_output_columns()
            + feature_add.get_output_columns()
            + feature_divide.get_output_columns()
        )
        assert result_df.collect() == feature_set_dataframe.collect()
        assert result_df.is_cached

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
