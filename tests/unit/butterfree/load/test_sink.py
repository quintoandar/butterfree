from unittest.mock import ANY, Mock

import pytest
from pyspark.sql.streaming import StreamingQuery

from butterfree.clients import SparkClient
from butterfree.load import Sink
from butterfree.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)
from butterfree.validations import BasicValidation


class TestSink:
    def test_validate(self, feature_set_dataframe, mocker):
        # given
        spark_client = SparkClient()
        writer = [
            HistoricalFeatureStoreWriter(),
            OnlineFeatureStoreWriter(),
        ]

        for w in writer:
            w.validate = mocker.stub("validate")

        feature_set = mocker.stub("feature_set")

        # when
        sink = Sink(writers=writer)
        sink.validate(
            dataframe=feature_set_dataframe,
            feature_set=feature_set,
            spark_client=spark_client,
        )

        # then
        for w in writer:
            w.validate.assert_called_once()

    def test_validate_false(self, feature_set_dataframe, mocker):
        # given
        spark_client = SparkClient()
        writer = [
            HistoricalFeatureStoreWriter(),
            OnlineFeatureStoreWriter(),
        ]

        for w in writer:
            w.validate = mocker.stub("validate")
            w.validate.side_effect = AssertionError("test")

        feature_set = mocker.stub("feature_set")

        # when
        sink = Sink(writers=writer)

        # then
        with pytest.raises(RuntimeError):
            sink.validate(
                dataframe=feature_set_dataframe,
                feature_set=feature_set,
                spark_client=spark_client,
            )

    def test_flush(self, feature_set_dataframe, mocker):
        # given
        spark_client = SparkClient()
        writer = [
            HistoricalFeatureStoreWriter(),
            OnlineFeatureStoreWriter(),
        ]

        for w in writer:
            w.build = mocker.stub("build")

        feature_set = mocker.stub("feature_set")
        feature_set.entity = "house"
        feature_set.name = "test"

        # when
        sink = Sink(writers=writer)
        sink.flush(
            dataframe=feature_set_dataframe,
            feature_set=feature_set,
            spark_client=spark_client,
        )

        # then
        for w in writer:
            w.build.assert_called_once()

    def test_flush_with_invalid_df(self, not_feature_set_dataframe, mocker):
        # given
        spark_client = SparkClient()
        writer = [
            HistoricalFeatureStoreWriter(),
            OnlineFeatureStoreWriter(),
        ]
        feature_set = mocker.stub("feature_set")
        feature_set.entity = "house"
        feature_set.name = "test"

        # when
        sink = Sink(writers=writer)

        # then
        with pytest.raises(ValueError):
            sink.flush(
                dataframe=not_feature_set_dataframe,
                feature_set=feature_set,
                spark_client=spark_client,
            )

    def test_flush_with_writers_list_empty(self):
        # given
        writer = []

        # then
        with pytest.raises(ValueError):
            Sink(writers=writer)

    def test_flush_streaming_df(
        self, feature_set, mocked_stream_df, online_feature_store_writer_stream
    ):
        """Testing the return of the streaming handlers by the sink."""
        # arrange
        spark_client = SparkClient()

        sink = Sink(
            writers=[online_feature_store_writer_stream],
            validation=Mock(spec=BasicValidation),
        )

        # act
        handlers = sink.flush(
            dataframe=mocked_stream_df,
            feature_set=feature_set,
            spark_client=spark_client,
        )

        # assert
        print(handlers[0])
        print(isinstance(handlers[0], StreamingQuery))
        for handler in handlers:
            assert isinstance(handler, StreamingQuery)

    def test_flush_with_multiple_online_writers(
        self, feature_set, feature_set_dataframe
    ):
        """Testing the flow of writing to a feature-set table and to an entity table."""
        # arrange
        spark_client = SparkClient()
        spark_client.write_dataframe = Mock()

        feature_set.entity = "my_entity"
        feature_set.name = "my_feature_set"

        online_feature_store_writer = OnlineFeatureStoreWriter()
        online_feature_store_writer.run_pre_hooks = Mock()
        online_feature_store_writer.run_pre_hooks.return_value = feature_set_dataframe
        online_feature_store_writer_on_entity = OnlineFeatureStoreWriter(
            write_to_entity=True
        )
        online_feature_store_writer_on_entity.run_pre_hooks = Mock()
        online_feature_store_writer_on_entity.run_pre_hooks.return_value = feature_set_dataframe

        sink = Sink(
            writers=[online_feature_store_writer, online_feature_store_writer_on_entity]
        )

        # act
        sink.flush(
            dataframe=feature_set_dataframe,
            feature_set=feature_set,
            spark_client=spark_client,
        )

        # assert
        spark_client.write_dataframe.assert_any_call(
            dataframe=ANY,
            format_=ANY,
            mode=ANY,
            **online_feature_store_writer.db_config.get_options(table="my_entity"),
        )

        spark_client.write_dataframe.assert_any_call(
            dataframe=ANY,
            format_=ANY,
            mode=ANY,
            **online_feature_store_writer_on_entity.db_config.get_options(table="my_feature_set"),
        )
