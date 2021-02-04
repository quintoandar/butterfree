from unittest.mock import Mock

from pyspark.sql.streaming import StreamingQuery

from butterfree.clients import SparkClient
from butterfree.load.online_sink import OnlineSink
from butterfree.load.writers.cassandra_writer import CassandraWriter


class TestOnlineSink:
    def test_flush(self, feature_set, feature_set_dataframe, mocker):
        # given
        spark_client = SparkClient()
        writer = [CassandraWriter(debug_mode=True)]

        # when
        sink = OnlineSink(writers=writer)
        sink.filter.return_value = feature_set_dataframe
        sink.flush(
            dataframe=feature_set_dataframe,
            feature_set=feature_set,
            spark_client=spark_client,
        )

        # then
        assert (
            spark_client.sql(
                "select count(1) from online_feature_store__feature_set"
            ).collect()[0]["count(1)"]
            == 2
        )

    def test_flush_streaming_df(self, feature_set):
        """Testing the return of the streaming handlers by the sink."""
        # arrange
        spark_client = SparkClient()

        mocked_stream_df = Mock()
        mocked_stream_df.isStreaming = True
        mocked_stream_df.writeStream = mocked_stream_df
        mocked_stream_df.trigger.return_value = mocked_stream_df
        mocked_stream_df.outputMode.return_value = mocked_stream_df
        mocked_stream_df.option.return_value = mocked_stream_df
        mocked_stream_df.foreachBatch.return_value = mocked_stream_df
        mocked_stream_df.start.return_value = Mock(spec=StreamingQuery)

        online_feature_store_writer = CassandraWriter()
        online_feature_store_writer_on_entity = CassandraWriter(
            write_to_entity=True
        )

        sink = OnlineSink(
            writers=[
                online_feature_store_writer,
                online_feature_store_writer_on_entity,
            ],
            filter_latest=False,
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
