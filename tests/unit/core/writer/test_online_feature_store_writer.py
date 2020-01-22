import pytest

from butterfree.core.writer import OnlineFeatureStoreWriter


class TestOnlineFeatureStoreWriter:
    def test_filter_latest(
        self, feature_set_dataframe, latest, cassandra_config, mocker
    ):
        # with
        spark_client = mocker.stub("spark_client")
        writer = OnlineFeatureStoreWriter(spark_client, cassandra_config)

        # when
        filtered = writer.filter_latest(feature_set_dataframe, id_columns=["id"])

        # then
        assert sorted(latest.collect()) == sorted(filtered.collect())

    def test_filter_latest_without_ts(
        self, feature_set_dataframe, latest, cassandra_config, mocker
    ):
        # with
        spark_client = mocker.stub("spark_client")
        writer = OnlineFeatureStoreWriter(spark_client, cassandra_config)

        # then
        with pytest.raises(KeyError, match="must have a 'ts' column"):
            _ = writer.filter_latest(
                feature_set_dataframe.drop("ts"), id_columns=["id"]
            )

    def test_filter_latest_without_id_columns(
        self, feature_set_dataframe, latest, cassandra_config, mocker
    ):
        # with
        spark_client = mocker.stub("spark_client")
        writer = OnlineFeatureStoreWriter(spark_client, cassandra_config)

        # then
        with pytest.raises(ValueError, match="must provide the unique identifiers"):
            _ = writer.filter_latest(feature_set_dataframe, id_columns=[])

        # then
        with pytest.raises(KeyError, match="not found"):
            _ = writer.filter_latest(
                feature_set_dataframe.drop("id"), id_columns=["id"]
            )

    def test_write(self, feature_set_dataframe, latest, cassandra_config, mocker):
        # with
        spark_client = mocker.stub("spark_client")
        spark_client.write_dataframe = mocker.stub("write_dataframe")
        writer = OnlineFeatureStoreWriter(spark_client, cassandra_config)

        # when
        writer.write(feature_set_dataframe, id_columns=["id"], name="test")

        # then
        spark_client.write_dataframe.assert_called_once()
        assert sorted(latest.collect()) == sorted(
            spark_client.write_dataframe.call_args[1]["dataframe"].collect()
        )
        assert (
            loader.db_config.mode == spark_client.write_dataframe.call_args[1]["mode"]
        )
        assert (
            writer.db_config.format_
            == spark_client.write_dataframe.call_args[1]["format"]
        )
        assert (
            writer.db_config.get_options(table="test")
            == spark_client.write_dataframe.call_args[1]["options"]
        )
