from unittest.mock import Mock

from butterfree.transform import FeatureSet


class TestFeatureSetMetadata:
    def test_build_metadata(
        self,
    ):
        # arrange
        key_id = Mock()
        key_id.build_metadata.return_value = "key"
        timestamp_c = Mock()
        timestamp_c.build_metadata.return_value = "timestamp"
        feature_add = Mock()
        feature_add.build_metadata.return_value = ["feature_add"]
        feature_divide = Mock()
        feature_divide.build_metadata.return_value = ["feature_divide"]

        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[key_id],
            timestamp=timestamp_c,
            features=[feature_add, feature_divide],
        )

        # act
        result = feature_set.build_metadata()

        # assert
        assert result.name == "name"
        assert result.entity == "entity"
        assert result.description == "description"
        assert result.features == ["key", "timestamp", "feature_add", "feature_divide"]
