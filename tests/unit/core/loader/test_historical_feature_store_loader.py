from butterfree.core.loader.historical_feature_store_loader import (
    HistoricalFeatureStoreLoader
)


class TestHistoricalFeatureStoreLoader:
    def test_load(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write = mocker.stub("write_dataframe")
        loader = HistoricalFeatureStoreLoader(spark_client)
        table_name = "test"

        # when
        loader.load(dataframe=feature_set_dataframe, name=table_name)

        # then
        spark_client.write.assert_called_once()

        assert sorted(feature_set_dataframe.collect()) == sorted(
            spark_client.write.call_args[1]["dataframe"].collect()
        )
        assert loader.DEFAULT_FORMAT == spark_client.write.call_args[1]["format_"]
        assert loader.DEFAULT_MODE == spark_client.write.call_args[1]["mode"]
        assert (
            loader.DEFAULT_PARTITION_BY
            == spark_client.write.call_args[1]["partition_by"]
        )
        assert table_name == spark_client.write.call_args[1]["name"]
