from unittest.mock import Mock, patch

from butterfree.transform.aggregated_feature_set import AggregatedFeatureSet


class TestAggregatedFeatureSetMetadata:
    def test_build_metadata(self):
        # arrange
        with patch.object(AggregatedFeatureSet, "__init__", lambda s, *a, **k: None):
            feature_set = AggregatedFeatureSet()

            # manually set attributes needed for the test
            feature_set.name = "name"
            feature_set.entity = "entity"
            feature_set.description = "description"

            window_mock = Mock()
            window_mock.build_metadata.return_value = "window_metadata"
            feature_set._windows = [window_mock]

            feature_set._pivot_values = ["pivot_value"]

            key_feature_mock = Mock()
            key_feature_mock.build_metadata.return_value = "key_metadata"
            feature_set.keys = [key_feature_mock]

            timestamp_feature_mock = Mock()
            timestamp_feature_mock.build_metadata.return_value = "timestamp_metadata"
            feature_set.timestamp = timestamp_feature_mock

            feature_mock = Mock()
            feature_mock.build_aggregated_feature_metadata.return_value = [
                "feature_metadata"
            ]
            feature_set.features = [feature_mock]

            # act
            result = feature_set.build_metadata()

            # assert
            feature_mock.build_aggregated_feature_metadata.assert_called_with(
                pivot_values=["pivot_value"], windows=[window_mock]
            )
            assert result.name == "name"
            assert result.entity == "entity"
            assert result.type == "AggregatedFeatureSet"
            assert result.description == "description"
            assert result.windows_definition == ["window_metadata"]
            assert result.features == [
                "key_metadata",
                "timestamp_metadata",
                "feature_metadata",
            ]
