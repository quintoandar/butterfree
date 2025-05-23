from unittest.mock import MagicMock

from butterfree.pipelines import FeatureSetPipeline
from butterfree.pipelines.feature_set_pipeline_metadata import (
    FeatureSetPipelineMetadata,
)
from butterfree.transform.aggregated_feature_set import AggregatedFeatureSet


class TestFeatureSetPipelineMetadata:
    def test_get_windows_definition_with_aggregated_feature_set(self):
        # Given
        feature_set = MagicMock(spec=AggregatedFeatureSet)
        feature_set._windows = [
            MagicMock(frame_boundaries=MagicMock(window_definition="30 days")),
            MagicMock(frame_boundaries=MagicMock(window_definition="60 days")),
        ]
        feature_set_pipeline = MagicMock(spec=FeatureSetPipeline)
        feature_set_pipeline.feature_set = feature_set

        # When
        result = FeatureSetPipelineMetadata._get_windows_definition(
            feature_set_pipeline
        )

        # Then
        assert result == ["30 days", "60 days"]

    def test_get_windows_definition_without_aggregated_feature_set(self):
        # Given
        feature_set = MagicMock()
        feature_set_pipeline = MagicMock(spec=FeatureSetPipeline)
        feature_set_pipeline.feature_set = feature_set

        # When
        result = FeatureSetPipelineMetadata._get_windows_definition(
            feature_set_pipeline
        )

        # Then
        assert result is None

    def test_is_incremental_with_incremental_reader(self):
        # Given
        reader = MagicMock(incremental_strategy=True)
        feature_set_pipeline = MagicMock(spec=FeatureSetPipeline)
        feature_set_pipeline.source.readers = [reader]

        # When
        result = FeatureSetPipelineMetadata._is_incremental(feature_set_pipeline)

        # Then
        assert result is True

    def test_is_incremental_without_incremental_reader(self):
        # Given
        reader = MagicMock(incremental_strategy=None)
        feature_set_pipeline = MagicMock(spec=FeatureSetPipeline)
        feature_set_pipeline.source.readers = [reader]

        # When
        result = FeatureSetPipelineMetadata._is_incremental(feature_set_pipeline)

        # Then
        assert result is False
