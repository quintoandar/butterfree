from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from butterfree.core.clients import SparkClient
from butterfree.core.configs.db import CassandraConfig
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
            for item in writer.db_config.get_options(table=feature_set.entity).items()
        )

    @pytest.mark.parametrize("has_checkpoint", [True, False])
    def test_write_stream(self, feature_set, has_checkpoint, monkeypatch):
        # arrange
        spark_client = SparkClient()
        spark_client.write_stream = Mock()
        spark_client.write_dataframe = Mock()
        spark_client.write_stream.return_value = Mock(spec=StreamingQuery)

        dataframe = Mock(spec=DataFrame)
        dataframe.isStreaming = True

        if has_checkpoint:
            monkeypatch.setenv("STREAM_CHECKPOINT_PATH", "test")

        cassandra_config = CassandraConfig(keyspace="feature_set")
        target_checkpoint_path = (
            "test/entity/feature_set"
            if cassandra_config.stream_checkpoint_path
            else None
        )

        writer = OnlineFeatureStoreWriter(cassandra_config)
        writer.filter_latest = Mock()

        # act
        stream_handler = writer.write(feature_set, dataframe, spark_client)

        # assert
        assert isinstance(stream_handler, StreamingQuery)
        spark_client.write_stream.assert_called_with(
            dataframe,
            processing_time=cassandra_config.stream_processing_time,
            output_mode=cassandra_config.stream_output_mode,
            checkpoint_path=target_checkpoint_path,
            format_=cassandra_config.format_,
            mode=cassandra_config.mode,
            **cassandra_config.get_options(table=feature_set.name),
        )
        writer.filter_latest.assert_not_called()
        spark_client.write_dataframe.assert_not_called()

    def test_get_feature_set_schema(
        self, cassandra_config, test_feature_set, expected_schema
    ):
        writer = OnlineFeatureStoreWriter(cassandra_config)
        schema = writer.get_db_schema(test_feature_set)

        assert schema == expected_schema
