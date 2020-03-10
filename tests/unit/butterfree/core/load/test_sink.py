import pytest

from butterfree.core.clients import SparkClient
from butterfree.core.load import Sink
from butterfree.core.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)


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
            w.write = mocker.stub("write")

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
            w.write.assert_called_once()

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
