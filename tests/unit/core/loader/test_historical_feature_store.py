from unittest.mock import Mock

import pytest

from butterfree.core.client.spark_client import SparkClient
from butterfree.core.loader.verify_dataframe import verify_column_ts
from butterfree.core.loader.historical_feature_store import HistoricalFeatureStoreLoader


class TestHistoricalLoader:
    def test_historical_loader(self, target_df):

        path = "path"
        entity = "house"
        format = "parquet"
        partition_column = "ts"

        mock_write = Mock()
        mock_df = Mock()
        mock_df.write = mock_write

        HistoricalFeatureStoreLoader.loader(
            path, entity, format, partition_column, mock_df)
        mock_write.saveAsTable.assert_called_with(
            mode="overwrite", format=format, partitionBy=partition_column, name="dataframe_temp")

    def test_verify_without_column_ts(self, target_df_wrong):
        with pytest.raises(ValueError):
            assert verify_column_ts(target_df_wrong)

    def test_verify_column_ts(self, target_df):
        assert(target_df == verify_column_ts(target_df))

    def test_write(self, target_df, spark_df_writer, kwargs):
        spark = SparkClient()
        spark_df_writer.write.return_value = target_df

        result_df = spark_client.write(spark_df_writer, **kwargs)
        assert(target_df == result_df)

    def test_write_invalid_params(self):
        df_writer = "not a spark df writer"

        with pytest.raises(ValueError):
            assert SparkClient.write(df_writer)
