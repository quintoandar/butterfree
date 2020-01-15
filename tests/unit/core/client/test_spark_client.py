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
        # singleton behaviour
        assert start_conn == None
        assert get_conn1 == get_conn2

    @pytest.mark.parametrize(
        "kwargs",
        [{"path": "path/to/file"}, {"path": "path/to/file", "header": True}, {},],
    )
    def test_load(self, target_df, spark_df_reader, kwargs):
        # arrange
        spark_client = SparkClient()
        spark_df_reader.load.return_value = target_df

        # act
        result_df = spark_client.load(spark_df_reader, **kwargs)

        # assert
        target_df = result_df

    def test_load_invalid_params(self):
        # arrange
        spark_client = SparkClient()

        # act and assert
        with pytest.raises(ValueError):
            assert spark_client.load(spark_df_reader="not a spark df reader")

    def test_get_records(self, target_df, mocked_spark):
        # arrange
        spark_client = SparkClient()
        spark_client._session = mocked_spark
        mocked_spark.sql.return_value = target_df

        # act
        result_df = spark_client.get_records("select * from my_table")

        # assert
        target_df = result_df
