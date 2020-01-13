from butterfree.core.loader import OnlineFeatureStoreLoader


class TestOnlineFeatureStoreLoader:
    def test_filter_latest(self, feature_set_dataframe, mocker):
        spark_client = mocker.stub("spark_client")
        spark_client.write_dataframe = mocker.stub("write_dataframe")
        loader = OnlineFeatureStoreLoader(spark_client)

        loader.load(feature_set_dataframe, id_columns=["id"], name="test")
        spark_client.write_dataframe.assert_called_once()
        assert (
            feature_set_dataframe.collect()
            != spark_client.write_dataframe.call_args[0][0].collect()
        )  # replace by expected dataframe
        assert loader.format_ == spark_client.write_dataframe.call_args[1]["format"]
        assert {
            **loader.get_options(table="test"),
        } == spark_client.write_dataframe.call_args[1]["options"]
