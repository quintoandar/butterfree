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
    def test_load(self, format, options, target_df, mocked_spark_load):
        # arrange
        spark_client = SparkClient()
        mocked_spark_load.load.return_value = target_df
        spark_client._session = mocked_spark_load

        # act
        result_df = spark_client.load(format, options)

        # assert
        mocked_spark_load.format.assert_called_once_with(format)
        mocked_spark_load.options.assert_called_once_with(**options)
        assert target_df == result_df

    @pytest.mark.parametrize(
        "format, options",
        [(None, {"path": "path/to/file"}), ("csv", "not a valid options")],
    )
    def test_load_invalid_params(self, format, options):
        # arrange
        spark_client = SparkClient()

        # act and assert
        with pytest.raises(ValueError):
            assert spark_client.load(format, options)
