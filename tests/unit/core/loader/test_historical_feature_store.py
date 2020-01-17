from unittest.mock import Mock

import pytest

from butterfree.core.client.spark_client import SparkClient
from butterfree.core.loader.verify_dataframe import verify_column_ts
from butterfree.core.loader.historical_feature_store import HistoricalFeatureStoreLoader


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
        assert (
                loader.DEFAULT_FORMAT
                == spark_client.write_table.call_args[1]["format_"]
        )
        assert (
                loader.DEFAULT_MODE
                == spark_client.write_table.call_args[1]["mode"]
        )
        assert (
                loader.DEFAULT_PARTITION_BY
                == spark_client.write_table.call_args[1]["partition_by"]
        )
        assert (
                table_name
                == spark_client.write_table.call_args[1]["name"]
        )

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
