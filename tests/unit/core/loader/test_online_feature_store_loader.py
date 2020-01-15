import pytest

from butterfree.core.loader import OnlineFeatureStoreLoader


class TestOnlineFeatureStoreLoader:
    def test_filter_latest(
        self, feature_set_dataframe, latest, cassandra_config, mocker
    ):
        # with
        spark_client = mocker.stub("spark_client")
        loader = OnlineFeatureStoreLoader(spark_client, cassandra_config)

        # when
        filtered = loader.filter_latest(feature_set_dataframe, id_columns=["id"])

        # then
        assert sorted(latest.collect()) == sorted(filtered.collect())

    def test_filter_latest_without_ts(
        self, feature_set_dataframe, latest, cassandra_config, mocker
    ):
        # with
        spark_client = mocker.stub("spark_client")
        loader = OnlineFeatureStoreLoader(spark_client, cassandra_config)

        # then
        with pytest.raises(KeyError, match="must have a 'ts' column"):
            _ = loader.filter_latest(
                feature_set_dataframe.drop("ts"), id_columns=["id"]
            )

    def test_filter_latest_without_id_columns(
        self, feature_set_dataframe, latest, cassandra_config, mocker
    ):
        # with
        spark_client = mocker.stub("spark_client")
        loader = OnlineFeatureStoreLoader(spark_client, cassandra_config)

        # then
        with pytest.raises(ValueError, match="must provide the unique identifiers"):
            _ = loader.filter_latest(feature_set_dataframe, id_columns=[])

        # then
        with pytest.raises(KeyError, match="not found"):
            _ = loader.filter_latest(
                feature_set_dataframe.drop("id"), id_columns=["id"]
            )

    def test_load(self, feature_set_dataframe, latest, cassandra_config, mocker):
        # with
        spark_client = mocker.stub("spark_client")
        spark_client.write_dataframe = mocker.stub("write_dataframe")
        loader = OnlineFeatureStoreLoader(spark_client, cassandra_config)

        # when
        loader.load(feature_set_dataframe, id_columns=["id"], name="test")

        # then
        spark_client.write_dataframe.assert_called_once()
        assert sorted(latest.collect()) == sorted(
            spark_client.write_dataframe.call_args[0][0].collect()
        )
        assert (
            loader.db_config.format_
            == spark_client.write_dataframe.call_args[1]["format"]
        )
        assert {
            **loader.db_config.options,
            "table": "test",
        } == spark_client.write_dataframe.call_args[1]["options"]
