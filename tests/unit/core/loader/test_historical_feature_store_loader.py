import pytest

from butterfree.core.loader import HistoricalFeatureStoreLoader


class TestHistoricalFeatureStoreLoader:
    def test_load(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
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
        assert table_name == spark_client.write_table.call_args[1]["table_name"]

    def test_load_with_df_invalid(
        self, feature_set_empty, feature_set_without_ts, mocker
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")

        loader = HistoricalFeatureStoreLoader(spark_client)
        table_name = "test"

        # then
        with pytest.raises(ValueError):
            assert loader.load(dataframe=feature_set_empty, name=table_name)

        with pytest.raises(ValueError):
            assert loader.load(dataframe=feature_set_without_ts, name=table_name)
