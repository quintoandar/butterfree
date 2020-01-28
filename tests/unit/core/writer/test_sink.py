import pytest

from butterfree.core.writer import HistoricalFeatureStoreWriter, Sink


class TestSink:
    def test_validate(self, feature_set_dataframe, mocker):
        # given
        writer = mocker.stub("writer")
        writer.validate = mocker.stub("validate")
        feature_set = mocker.stub("feature_set")

        # when
        sink = Sink(feature_set=feature_set, writers=[writer])
        sink.validate(feature_set=feature_set, dataframe=feature_set_dataframe)

        # then
        writer.validate.assert_called_once()

    def test_flush(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        writer = HistoricalFeatureStoreWriter
        writer.write = mocker.stub("write")
        writer.validate = mocker.stub("validate")
        feature_set = mocker.stub("feature_set")
        feature_set.name = "test"

        # when
        sink = Sink(feature_set=feature_set, writers=[writer])
        sink.flush(dataframe=feature_set_dataframe, spark_client=spark_client)

        # then
        writer.write.assert_called_once()
        writer.validate.assert_called_once()

    def test_flush_with_invalid_df(self, feature_set_dataframe, mocker):
        spark_client = mocker.stub("spark_client")
        writer = HistoricalFeatureStoreWriter
        feature_set = mocker.stub("feature_set")
        feature_set.name = "test"

        # when
        sink = Sink(feature_set=feature_set, writers=[writer])

        # then
        with pytest.raises(ValueError):
            assert sink.flush(dataframe="oi", spark_client=spark_client)
