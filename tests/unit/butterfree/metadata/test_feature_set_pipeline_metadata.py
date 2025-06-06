from unittest.mock import Mock, patch

from butterfree.pipelines.feature_set_pipeline import FeatureSetPipeline


class TestFeatureSetPipelineMetadata:
    def test_build_metadata(self):
        # arrange
        with patch.object(FeatureSetPipeline, "__init__", lambda s, **k: None):
            pipeline = FeatureSetPipeline()

            # manually set attributes needed for the test
            feature_set_mock = Mock()
            feature_set_mock.build_metadata.return_value = "feature_set_metadata"
            pipeline.feature_set = feature_set_mock

            source_mock = Mock()
            reader_mock = Mock()
            reader_mock.build_metadata.return_value = "reader_metadata"
            source_mock.readers = [reader_mock]
            pipeline.source = source_mock

            sink_mock = Mock()
            writer_mock = Mock()
            writer_mock.build_metadata.return_value = "writer_metadata"
            sink_mock.writers = [writer_mock]
            pipeline.sink = sink_mock

            # act
            result = pipeline.build_metadata()

            # assert
            assert result.feature_set == "feature_set_metadata"
            assert result.readers == ["reader_metadata"]
            assert result.writers == ["writer_metadata"]
