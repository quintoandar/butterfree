import pytest

from butterfree.core.load.writers import OnlineFeatureStoreWriter


class TestOnlineFeatureStoreWriter:
    def test_filter_latest(
        self, feature_set_dataframe, latest_feature_set_dataframe, cassandra_config
    ):
        # with
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # when
        result_df = writer.filter_latest(feature_set_dataframe, id_columns=["id"])
        sort_columns = result_df.schema.fieldNames()

        # then
        assert sorted(
            latest_feature_set_dataframe.select(*sort_columns).collect()
        ) == sorted(result_df.select(*sort_columns).collect())

    def test_filter_latest_without_ts(
        self, feature_set_dataframe_without_ts, cassandra_config
    ):
        # with
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # then
        with pytest.raises(KeyError):
            _ = writer.filter_latest(
                feature_set_dataframe_without_ts, id_columns=["id"]
            )

    def test_filter_latest_without_id_columns(
        self, feature_set_dataframe, cassandra_config
    ):
        # with
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # then
        with pytest.raises(ValueError, match="must provide the unique identifiers"):
            _ = writer.filter_latest(feature_set_dataframe, id_columns=[])

        # then
        with pytest.raises(KeyError, match="not found"):
            _ = writer.filter_latest(
                feature_set_dataframe.drop("id"), id_columns=["id"]
            )

    def test_write(
        self,
        feature_set_dataframe,
        latest_feature_set_dataframe,
        cassandra_config,
        mocker,
        feature_set,
    ):
        # with
        spark_client = mocker.stub("spark_client")
        spark_client.write_dataframe = mocker.stub("write_dataframe")
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # when
        writer.write(feature_set, feature_set_dataframe, spark_client)

        # then
        spark_client.write_dataframe.assert_called_once()

        assert sorted(latest_feature_set_dataframe.collect()) == sorted(
            spark_client.write_dataframe.call_args[1]["dataframe"].collect()
        )
        assert (
            writer.db_config.mode == spark_client.write_dataframe.call_args[1]["mode"]
        )
        assert (
            writer.db_config.format_
            == spark_client.write_dataframe.call_args[1]["format_"]
        )
        # assert if all additional options got from db_config
        # are in the called args in write_dataframe
        assert all(
            item in spark_client.write_dataframe.call_args[1].items()
            for item in writer.db_config.get_options(table=feature_set.name).items()
        )

    def test_validate(
        self, feature_set_dataframe, cassandra_config, mocker, feature_set
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read = mocker.stub("read")
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # when
        writer.validate(feature_set, feature_set_dataframe, spark_client)

        # then
        spark_client.read.assert_called_once()

    @pytest.mark.parametrize(
        "format_, table_name",
        [(None, "table"), ("org.apache.spark.sql.cassandra", None), (1, 123)],
    )
    def test_validate_invalid_params(
        self, feature_set_dataframe, format_, table_name, mocker,
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read = mocker.stub("read")
        feature_set = mocker.stub("feature_set")
        feature_set.name = table_name
        feature_set.key_columns = ["id"]

        db_config = mocker.stub("db_config")
        db_config.format_ = format_

        writer = OnlineFeatureStoreWriter(db_config)

        # then
        with pytest.raises(ValueError):
            writer.validate(feature_set, feature_set_dataframe, spark_client)
