from butterfree.constants import DataType, columns
from butterfree.metadata.feature_metadata import FeatureMetadata
from butterfree.transform.features import TimestampFeature


class TestTimestampFeatureMetadata:
    def test_build_metadata(self):
        # arrange
        ts_feature = TimestampFeature()

        # act
        metadata = ts_feature.build_metadata()

        # assert
        assert isinstance(metadata, FeatureMetadata)
        assert metadata.name == columns.TIMESTAMP_COLUMN
        assert metadata.data_type == DataType.TIMESTAMP.name
        assert metadata.primary_key is False
        assert metadata.description == "Time tag for the state of all features."
