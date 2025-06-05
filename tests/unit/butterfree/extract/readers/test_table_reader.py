import pytest

from butterfree.dataframe_service import IncrementalStrategy
from butterfree.extract.readers import TableReader
from butterfree.metadata.reader_metadata import TableReaderMetadata


class TestTableReader:
    @pytest.mark.parametrize(
        "database, table",
        [
            ("database", 123),
            (
                123,
                None,
            ),
        ],
    )
    def test_init_invalid_params(self, database, table):
        # act and assert
        with pytest.raises(ValueError):
            TableReader("id", table, database)

    def test_consume(self, spark_client, target_df):
        # arrange
        database = "test_database"
        table = "test_table"
        spark_client.read_table.return_value = target_df
        table_reader = TableReader("test", table, database)

        # act
        output_df = table_reader.consume(spark_client)

        # assert
        spark_client.read_table.assert_called_once_with(table, database)
        assert target_df.collect() == output_df.collect()

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
