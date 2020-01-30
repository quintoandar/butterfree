import pytest

from butterfree.core.writer import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
    Sink,
)


class TestSink:
    def test_validate(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        writer = [
            HistoricalFeatureStoreWriter(spark_client),
            OnlineFeatureStoreWriter(spark_client),
        ]

        for w in writer:
            w.validate = mocker.stub("validate")

        feature_set = mocker.stub("feature_set")

        # when
        sink = Sink(writers=writer)
        sink.validate(dataframe=feature_set_dataframe, feature_set=feature_set)

        # then
        for w in writer:
            w.validate.assert_called_once()

    def test_validate_false(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        writer = [
            HistoricalFeatureStoreWriter(spark_client),
            OnlineFeatureStoreWriter(spark_client),
        ]

        for w in writer:
            w.validate = mocker.stub("validate")
            w.validate.return_value = False

        feature_set = mocker.stub("feature_set")

        # when
        sink = Sink(writers=writer)

        # then
        with pytest.raises(RuntimeError):
            sink.validate(dataframe=feature_set_dataframe, feature_set=feature_set)

    def test_flush(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        writer = [
            HistoricalFeatureStoreWriter(spark_client),
            OnlineFeatureStoreWriter(spark_client),
        ]

        for w in writer:
            w.write = mocker.stub("write")

        feature_set = mocker.stub("feature_set")
        feature_set.entity = "house"
        feature_set.name = "test"

        # when
        sink = Sink(writers=writer)
        sink.flush(dataframe=feature_set_dataframe, feature_set=feature_set)

        # then
        for w in writer:
            w.write.assert_called_once()

    def test_flush_with_invalid_df(self, feature_sets, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        writer = [
            HistoricalFeatureStoreWriter(spark_client),
            OnlineFeatureStoreWriter(spark_client),
        ]
        feature_set = mocker.stub("feature_set")
        feature_set.entity = "house"
        feature_set.name = "test"

        # when
        sink = Sink(writers=writer)

        # then
        with pytest.raises(ValueError):
            sink.flush(dataframe=feature_sets, feature_set=feature_set)

    def test_flush_with_writers_list_empty(self, mocker):
        # given
        writer = []
        feature_set = mocker.stub("feature_set")
        feature_set.entity = "house"
        feature_set.name = "test"

        # then
        with pytest.raises(ValueError):
            Sink(writers=writer)
