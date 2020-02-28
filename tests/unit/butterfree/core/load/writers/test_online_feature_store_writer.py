import pytest
from testing import check_dataframe_equality

from butterfree.core.load.writers import OnlineFeatureStoreWriter


class TestOnlineFeatureStoreWriter:
    def test_filter_latest(self, input_df, latest_df, cassandra_config):
        # with
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # when
        output_df = writer.filter_latest(input_df, id_columns=["id"])

        # then
        assert check_dataframe_equality(output_df, latest_df)

    def test_filter_latest_without_ts(self, df_without_ts, cassandra_config):
        # with
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # then
        with pytest.raises(KeyError):
            _ = writer.filter_latest(df_without_ts, id_columns=["id"])

    def test_filter_latest_without_id_columns(self, input_df, cassandra_config):
        # with
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # then
        with pytest.raises(ValueError, match="must provide the unique identifiers"):
            _ = writer.filter_latest(input_df, id_columns=[])

        # then
        with pytest.raises(KeyError, match="not found"):
            _ = writer.filter_latest(input_df.drop("id"), id_columns=["id"])

    def test_write(
        self, input_df, latest_df, cassandra_config, mocker, feature_set,
    ):
        # with
        spark_client = mocker.stub("spark_client")
        spark_client.write_dataframe = mocker.stub("write_dataframe")
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # when
        writer.write(feature_set, input_df, spark_client)

        # then
        spark_client.write_dataframe.assert_called_once()

        assert sorted(latest_df.collect()) == sorted(
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

    def test_validate(self, input_df, cassandra_config, mocker, feature_set):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read = mocker.stub("read")
        writer = OnlineFeatureStoreWriter(cassandra_config)

        # when
        writer.validate(feature_set, input_df, spark_client)

        # then
        spark_client.read.assert_called_once()

    @pytest.mark.parametrize(
        "format_, table_name",
        [(None, "table"), ("org.apache.spark.sql.cassandra", None), (1, 123)],
    )
    def test_validate_invalid_params(
        self, input_df, format_, table_name, mocker,
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
            writer.validate(feature_set, input_df, spark_client)
