import pytest
from pyspark.sql import DataFrame

from butterfree.core.client import SparkClient


def create_temp_view(dataframe: DataFrame, name):
    dataframe.createOrReplaceTempView(name)


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
        "format, options, stream",
        [
            ("parquet", {"path": "path/to/file"}, False),
            ("csv", {"path": "path/to/file", "header": True}, False),
            ("json", {"path": "path/to/file"}, True),
        ],
    )
    def test_read(self, format, options, stream, target_df, mocked_spark_read):
        # arrange
        spark_client = SparkClient()
        mocked_spark_read.load.return_value = target_df
        spark_client._session = mocked_spark_read

        # act
        result_df = spark_client.read(format, options, stream)

        # assert
        mocked_spark_read.format.assert_called_once_with(format)
        mocked_spark_read.options.assert_called_once_with(**options)
        assert target_df.collect() == result_df.collect()

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

    def test_sql(self, target_df):
        # arrange
        spark_client = SparkClient()
        create_temp_view(target_df, "test")

        # act
        result_df = spark_client.sql("select * from test")

        # assert
        assert result_df.collect() == target_df.collect()

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
            spark_client.read_table(database, table)

    @pytest.mark.parametrize(
        "format", [("parquet"), ("csv")],
    )
    def test_write_dataframe(self, format, mocked_spark_write):
        SparkClient.write_dataframe(mocked_spark_write, format)
        mocked_spark_write.save.assert_called_with(format=format)

    @pytest.mark.parametrize(
        "format", [(None), (1)],
    )
    def test_write_dataframe_invalid_params(self, target_df, format):
        # arrange
        spark_client = SparkClient()

        # act and assert
        with pytest.raises(ValueError):
            assert spark_client.write_dataframe(dataframe=target_df, format_=format)

    def test_write_table(self, mocked_spark_write):
        SparkClient.write_table(mocked_spark_write, "test")
        mocked_spark_write.saveAsTable.assert_called_with(
            mode=None, format=None, partitionBy=None, name="test"
        )

    def test_write_table_with_invalid_params(self):
        df_writer = "not a spark df writer"
        name = None

        with pytest.raises(ValueError):
            assert SparkClient.write_table(dataframe=df_writer, name=name)
