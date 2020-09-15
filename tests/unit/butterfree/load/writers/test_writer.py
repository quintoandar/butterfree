from pyspark.sql.streaming import StreamingQuery

from butterfree.clients import SparkClient
from butterfree.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)
from butterfree.testing.dataframe import assert_dataframe_equality


class TestWriter:
    def test_build(
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

        # act
        historical_writer.build(feature_set, feature_set_dataframe, spark_client)

        # assert
        historical_writer.write.assert_called_once()

    def test_build_in_debug_mode(
        self, feature_set_dataframe, feature_set, spark_session,
    ):
        # given
        spark_client = SparkClient()
        writer = HistoricalFeatureStoreWriter(debug_mode=True)

        # when
        writer.build(
            feature_set=feature_set,
            dataframe=feature_set_dataframe,
            spark_client=spark_client,
        )
        result_df = spark_session.table(f"{feature_set.name}")

        # then
        assert_dataframe_equality(feature_set_dataframe, result_df)

    def test_build_in_debug_and_stream_mode(
        self, feature_set, spark_session, mocked_stream_df
    ):
        # arrange
        spark_client = SparkClient()

        writer = OnlineFeatureStoreWriter(debug_mode=True)

        # act
        handler = writer.build(
            feature_set=feature_set,
            dataframe=mocked_stream_df,
            spark_client=spark_client,
        )

        # assert
        mocked_stream_df.format.assert_called_with("memory")
        mocked_stream_df.queryName.assert_called_with(f"{feature_set.name}")
        assert isinstance(handler, StreamingQuery)
