import pytest

from butterfree.core.source import TableSource


class TestTableeSource:
    @pytest.mark.parametrize(
        "database, table", [(None, "table"), ("database", 123), (123, None,)],
    )
    def test_init_invalid_params(self, database, table, spark_client):
        # act and assert
        with pytest.raises(ValueError):
            TableSource("id", spark_client, database, table)

    def test_consume(self, spark_client, target_df):
        # arrange
        database = "test_database"
        table = "test_table"
        spark_client.read_table.return_value = target_df
        table_source = TableSource("test", spark_client, database, table)

        # act
        output_df = table_source.consume()

        # assert
        spark_client.read_table.assert_called_once_with(database, table)
        assert target_df.collect() == output_df.collect()
