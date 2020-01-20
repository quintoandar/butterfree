import pytest

from butterfree.core.client.spark_client import SparkClient
from butterfree.core.loader.historical_feature_store_loader import (
    HistoricalFeatureStoreLoader
)
from butterfree.core.loader.verify_dataframe import verify_column_ts


class TestHistoricalLoader:
    def test_load(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_dataframe")
        loader = HistoricalFeatureStoreLoader(spark_client)
        table_name = "test"
        # when
        loader.load(dataframe=feature_set_dataframe, name=table_name)

        # then
        spark_client.write_table.assert_called_once()
        assert sorted(feature_set_dataframe.collect()) == sorted(
            spark_client.write_table.call_args[1]["dataframe"].collect()
        )
        assert loader.DEFAULT_FORMAT == spark_client.write_table.call_args[1]["format_"]
        assert loader.DEFAULT_MODE == spark_client.write_table.call_args[1]["mode"]
        assert (
            loader.DEFAULT_PARTITION_BY
            == spark_client.write_table.call_args[1]["partition_by"]
        )
        assert table_name == spark_client.write_table.call_args[1]["name"]

    def test_verify_without_column_ts(self, feature_set_without_ts):
        with pytest.raises(ValueError):
            assert verify_column_ts(feature_set_without_ts)

    def test_verify_column_ts(self, feature_set_dataframe):
        df = verify_column_ts(feature_set_dataframe)
        assert feature_set_dataframe == df

    def test_write_table_with_invalid_params(self):
        df_writer = "not a spark df writer"
        name = "test"

        with pytest.raises(ValueError):
            assert SparkClient.write_table(dataframe=df_writer, name=name)
