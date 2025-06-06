from butterfree.dataframe_service import IncrementalStrategy
from butterfree.extract.readers import FileReader
from butterfree.metadata.reader_metadata import FileReaderMetadata


class TestFileReaderMetadata:
    def test_build_metadata(self):
        # given
        file_reader = FileReader(
            id="file_reader",
            path="path",
            format="format",
        )

        # when
        metadata = file_reader.build_metadata()

        # then
        assert isinstance(metadata, FileReaderMetadata)
        assert metadata.path == file_reader.path
        assert metadata.format == file_reader.format
        assert not metadata.incremental_strategy

    def test_build_metadata_with_incremental_strategy(self):
        # given
        file_reader = FileReader(
            id="file_reader",
            path="path",
            format="format",
        ).with_incremental_strategy(IncrementalStrategy(column="timestamp"))

        # when
        metadata = file_reader.build_metadata()

        # then
        assert isinstance(metadata, FileReaderMetadata)
        assert metadata.path == file_reader.path
        assert metadata.format == file_reader.format
        assert metadata.incremental_strategy
