from unittest.mock import MagicMock

from butterfree.constants.data_type import DataType
from butterfree.dataframe_service.incremental_strategy import IncrementalStrategy
from butterfree.extract.readers.file_reader import FileReader
from butterfree.extract.readers.table_reader import TableReader
from butterfree.load.writers.historical_feature_store_writer import (
    HistoricalFeatureStoreWriter,
)
from butterfree.load.writers.online_feature_store_writer import OnlineFeatureStoreWriter
from butterfree.pipelines import FeatureSetPipeline
from butterfree.pipelines.feature_set_pipeline_metadata import Catalog, Column, Metadata
from butterfree.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.transform.features.feature import Feature
from butterfree.transform.features.key_feature import KeyFeature
from butterfree.transform.features.timestamp_feature import TimestampFeature


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
        result = Metadata._get_windows_definition(feature_set_pipeline)

        # Then
        assert result == ["30 days", "60 days"]

    def test_get_windows_definition_without_aggregated_feature_set(self):
        # Given
        feature_set = MagicMock()
        feature_set_pipeline = MagicMock(spec=FeatureSetPipeline)
        feature_set_pipeline.feature_set = feature_set

        # When
        result = Metadata._get_windows_definition(feature_set_pipeline)

        # Then
        assert result is None

    def test_is_incremental_with_incremental_reader(self):
        # Given
        reader = MagicMock(incremental_strategy=True)
        feature_set_pipeline = MagicMock(spec=FeatureSetPipeline)
        feature_set_pipeline.source.readers = [reader]

        # When
        result = Metadata._is_incremental(feature_set_pipeline)

        # Then
        assert result is True

    def test_is_incremental_without_incremental_reader(self):
        # Given
        reader = MagicMock(incremental_strategy=None)
        feature_set_pipeline = MagicMock(spec=FeatureSetPipeline)
        feature_set_pipeline.source.readers = [reader]

        # When
        result = Metadata._is_incremental(feature_set_pipeline)

        # Then
        assert result is False

    def test_from_feature_set(self):
        # Given

        # Setup readers
        table_reader1 = TableReader(
            id="1", database="test_database_1", table="test_table_1"
        )
        table_reader2 = TableReader(
            id="2", database="test_database_2", table="test_table_2"
        ).with_incremental_strategy(IncrementalStrategy(column="timestamp"))
        file_reader = FileReader(id="3", path="/path/to/file", format="parquet")

        # Setup feature set
        keys = [
            KeyFeature(
                name="id",
                description="The user's Main ID or device ID",
                dtype=DataType.BIGINT,
            )
        ]
        timestamp = TimestampFeature()
        features = [
            Feature(
                name="type",
                description="Real estate type (Null, Apartamento, Casa, "
                "CasaCondominio, StudioOuKitchenette).",
                dtype=DataType.STRING,
            )
        ]
        _windows = [
            MagicMock(frame_boundaries=MagicMock(window_definition="30 days")),
            MagicMock(frame_boundaries=MagicMock(window_definition="60 days")),
        ]
        entity = "test_entity"
        name = "test_feature_set"
        description = "Test feature set description"

        # Setup pipeline
        feature_set_pipeline = MagicMock(spec=FeatureSetPipeline)
        feature_set_pipeline.source.readers = [
            table_reader1,
            table_reader2,
            file_reader,
        ]
        feature_set_pipeline.sink.writers = [
            OnlineFeatureStoreWriter(),
            HistoricalFeatureStoreWriter(),
        ]
        feature_set_pipeline.feature_set = type(
            "AggregatedFeatureSet", (MagicMock,), {"__class__": AggregatedFeatureSet}
        )()
        feature_set_pipeline.feature_set.keys = keys
        feature_set_pipeline.feature_set.timestamp = timestamp
        feature_set_pipeline.feature_set.features = features
        feature_set_pipeline.feature_set._windows = _windows
        feature_set_pipeline.feature_set.entity = entity
        feature_set_pipeline.feature_set.name = name
        feature_set_pipeline.feature_set.description = description

        # When
        metadata = Metadata.from_pipeline(feature_set_pipeline)

        # Then
        assert metadata == Metadata(
            feature_set_pipeline=feature_set_pipeline,
            is_incremental=True,
            readers=[
                reader.get_metadata() for reader in feature_set_pipeline.source.readers
            ],
            catalog=Catalog(
                feature_set_name=feature_set_pipeline.feature_set.name,
                description=feature_set_pipeline.feature_set.description,
                columns=[
                    Column(
                        name=feature.name,
                        data_type=feature.dtype,
                        primary_key=False,
                    )
                    for feature in feature_set_pipeline.feature_set.features
                ],
            ),
            entity=feature_set_pipeline.feature_set.entity,
            feature_set_type="AggregatedFeatureSet",
            windows_definition=["30 days", "60 days"],
            key_features=[
                Column(
                    name=feature.name,
                    data_type=feature.dtype,
                    primary_key=True,
                )
                for feature in feature_set_pipeline.feature_set.keys
            ],
            writers=[
                writer.get_metadata() for writer in feature_set_pipeline.sink.writers
            ],
        )
