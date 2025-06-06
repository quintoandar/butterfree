from butterfree.constants import DataType
from butterfree.metadata.feature_metadata import FeatureMetadata
from butterfree.transform.features import KeyFeature


class TestKeyFeatureMetadata:
    def test_build_metadata(self):
        # arrange
        key_feature = KeyFeature(
            name="my_key",
            description="a key feature",
            dtype=DataType.INTEGER,
        )

        # act
        metadata = key_feature.build_metadata()

        # assert
        assert isinstance(metadata, FeatureMetadata)
        assert metadata.name == "my_key"
        assert metadata.data_type == "INTEGER"
        assert metadata.primary_key is True
        assert metadata.description == "a key feature"
