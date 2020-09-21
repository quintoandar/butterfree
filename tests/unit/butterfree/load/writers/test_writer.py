from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from butterfree.clients import SparkClient
from butterfree.configs.db import CassandraConfig
from butterfree.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)
from butterfree.testing.dataframe import assert_dataframe_equality


class TestWriter:
    def test_write(
        self,
        feature_set_dataframe,
        historical_feature_set_dataframe,
        mocker,
        feature_set,
    ):
        # arrange
        spark_client = SparkClient()
        spark_client.conn.conf.set(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )
        historical_writer = HistoricalFeatureStoreWriter()
        historical_writer.write = mocker.stub("write")
        spark_client.write_dataframe = mocker.stub("write_dataframe")
        spark_client.add_table_partitions = mocker.stub("add_table_partitions")

        # act
        historical_writer.write(feature_set, feature_set_dataframe, spark_client)

        # assert
        historical_writer.write.assert_called_once()

    def test_write_in_debug_mode(
        self,
        feature_set_dataframe,
        feature_set,
        spark_session,
        historical_feature_set_dataframe,
    ):
        # given
        spark_client = SparkClient()

        writer = HistoricalFeatureStoreWriter(debug_mode=True)
        spark_session.sql("CREATE DATABASE IF NOT EXISTS {}".format(writer.database))
        spark_session.sql(
            "CREATE TABLE {}.{} (id bigint, "
            "timestamp string, "
            "feature bigint) PARTITIONED BY (year int, month int, day int)".format(
                writer.database, feature_set.name
            )
        )
        # when
        writer.write(
            feature_set=feature_set,
            dataframe=feature_set_dataframe,
            spark_client=spark_client,
        )
        result_df = spark_session.table(f"historical_feature_store__{feature_set.name}")

        # then
        assert_dataframe_equality(historical_feature_set_dataframe, result_df)

    def test_write_in_debug_and_stream_mode(
        self, feature_set, spark_session, mocked_stream_df
    ):
        # arrange
        spark_client = SparkClient()

        writer = OnlineFeatureStoreWriter(debug_mode=True)
        writer.run_pre_hooks = Mock()
        writer.run_pre_hooks.return_value = mocked_stream_df

        # act
        handler = writer.write(
            feature_set=feature_set,
            dataframe=mocked_stream_df,
            spark_client=spark_client,
        )

        # assert
        mocked_stream_df.format.assert_called_with("memory")
        mocked_stream_df.queryName.assert_called_with(
            f"online_feature_store__{feature_set.name}"
        )
        assert isinstance(handler, StreamingQuery)

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
            "test/entity/feature_set_test"
            if cassandra_config.stream_checkpoint_path
            else None
        )

        writer = OnlineFeatureStoreWriter(cassandra_config)
        writer.run_pre_hooks = Mock()
        writer.run_pre_hooks.return_value = dataframe

        # act
        stream_handler = writer.write(feature_set, dataframe, spark_client)

        # assert
        assert isinstance(stream_handler, StreamingQuery)
        spark_client.write_stream.assert_any_call(
            dataframe,
            processing_time=cassandra_config.stream_processing_time,
            output_mode=cassandra_config.stream_output_mode,
            checkpoint_path=target_checkpoint_path,
            format_=cassandra_config.format_,
            mode=cassandra_config.mode,
            **cassandra_config.get_options(table=feature_set.name),
        )
        spark_client.write_dataframe.assert_not_called()

    def test_write_stream_on_entity(self, feature_set, monkeypatch):
        """Test write method with stream dataframe and write_to_entity enabled.

        The main purpose of this test is assert the correct setup of stream checkpoint
        path and if the target table name is the entity.

        """

        # arrange
        spark_client = SparkClient()
        spark_client.write_stream = Mock()
        spark_client.write_stream.return_value = Mock(spec=StreamingQuery)

        dataframe = Mock(spec=DataFrame)
        dataframe.isStreaming = True

        feature_set.entity = "my_entity"
        feature_set.name = "my_feature_set"
        monkeypatch.setenv("STREAM_CHECKPOINT_PATH", "test")
        target_checkpoint_path = "test/my_entity/my_feature_set__on_entity"

        writer = OnlineFeatureStoreWriter(write_to_entity=True)
        writer.run_pre_hooks = Mock()
        writer.run_pre_hooks.return_value = dataframe

        # act
        stream_handler = writer.write(feature_set, dataframe, spark_client)

        # assert
        assert isinstance(stream_handler, StreamingQuery)
        spark_client.write_stream.assert_called_with(
            dataframe,
            processing_time=writer.db_config.stream_processing_time,
            output_mode=writer.db_config.stream_output_mode,
            checkpoint_path=target_checkpoint_path,
            format_=writer.db_config.format_,
            mode=writer.db_config.mode,
            **writer.db_config.get_options(table="my_entity"),
        )
