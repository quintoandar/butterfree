import pytest

from butterfree.extract.readers import TableReader


class TestTableReader:
    @pytest.mark.parametrize(
        "database, table", [("database", 123), (123, None,)],
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
