import pytest

from butterfree.core.client import SparkClient


class TestSparkClient:
    def test_conn(self):
        # arrange
        spark_client = SparkClient()

        # act
        start_conn = spark_client._session
        get_conn1 = spark_client.conn
        get_conn2 = spark_client.conn

        # assert
        assert start_conn is None
        assert get_conn1 == get_conn2

    @pytest.mark.parametrize(
        "format, options",
        [
            ("parquet", {"path": "path/to/file"}),
            ("csv", {"path": "path/to/file", "header": True}),
        ],
    )
    def test_read(self, format, options, target_df, mocked_spark_read):
        # arrange
        spark_client = SparkClient()
        mocked_spark_read.load.return_value = target_df
        spark_client._session = mocked_spark_read

        # act
        result_df = spark_client.read(format, options)

        # assert
        mocked_spark_read.format.assert_called_once_with(format)
        mocked_spark_read.options.assert_called_once_with(**options)
        assert target_df == result_df

    @pytest.mark.parametrize(
        "format, options",
        [(None, {"path": "path/to/file"}), ("csv", "not a valid options")],
    )
    def test_read_invalid_params(self, format, options):
        # arrange
        spark_client = SparkClient()

        # act and assert
        with pytest.raises(ValueError):
            assert spark_client.read(format, options)

    def test_read_table(self, target_df, mocked_spark_read):
        # arrange
        database = "default"
        table = "test_table"
        spark_client = SparkClient()
        mocked_spark_read.table.return_value = target_df
        spark_client._session = mocked_spark_read

        # act
        result_df = spark_client.read_table(database, table)

        # assert
        mocked_spark_read.table.assert_called_once_with("{}.{}".format(database, table))
        assert target_df == result_df

    @pytest.mark.parametrize(
        "database, table", [(None, "table"), ("database", None), ("database", 123)],
    )
    def test_read_table_invalid_params(self, database, table):
        # arrange
        spark_client = SparkClient()

        # act and assert
        with pytest.raises(ValueError):
            assert spark_client.read_table(database, table)
