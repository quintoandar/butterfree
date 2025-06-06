from butterfree.dataframe_service import IncrementalStrategy
from butterfree.extract.readers import TableReader
from butterfree.metadata.reader_metadata import TableReaderMetadata


class TestTableReaderMetadata:
    def test_build_metadata(self):
        # given
        table_reader = TableReader(
            id="table_reader",
            database="db",
            table="table",
        )

        # when
        metadata = table_reader.build_metadata()

        # then
        assert isinstance(metadata, TableReaderMetadata)
        assert metadata.database == table_reader.database
        assert metadata.table == table_reader.table
        assert not metadata.incremental_strategy

    def test_build_metadata_with_incremental_strategy(self):
        # given
        table_reader = TableReader(
            id="table_reader",
            database="db",
            table="table",
        ).with_incremental_strategy(IncrementalStrategy(column="timestamp"))

        # when
        metadata = table_reader.build_metadata()

        # then
        assert isinstance(metadata, TableReaderMetadata)
        assert metadata.database == table_reader.database
        assert metadata.table == table_reader.table
        assert metadata.incremental_strategy
