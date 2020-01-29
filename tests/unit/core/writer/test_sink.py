import pytest

from butterfree.core.writer import HistoricalFeatureStoreWriter, Sink


class TestSink:
    def test_validate(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        writer = HistoricalFeatureStoreWriter
        writer.validate = mocker.stub("validate")
        feature_set = mocker.stub("feature_set")

        # when
        sink = Sink(
            feature_set=feature_set, writers=[writer], spark_client=spark_client
        )
        sink.validate(dataframe=feature_set_dataframe)

        # then
        writer.validate.assert_called_once()

    def test_validate_false(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        writer = HistoricalFeatureStoreWriter
        writer.validate = mocker.stub("validate")
        writer.validate.return_value = False
        feature_set = mocker.stub("feature_set")

        # when
        sink = Sink(
            feature_set=feature_set, writers=[writer], spark_client=spark_client
        )

        # then
        with pytest.raises(ValueError):
            sink.validate(dataframe=feature_set_dataframe)

    def test_flush(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        writer = HistoricalFeatureStoreWriter
        writer.write = mocker.stub("write")
        feature_set = mocker.stub("feature_set")
        feature_set.entity = "house"
        feature_set.name = "test"

        # when
        sink = Sink(
            feature_set=feature_set, writers=[writer], spark_client=spark_client
        )
        sink.flush(dataframe=feature_set_dataframe)

        # then
        writer.write.assert_called_once()

    def test_flush_with_invalid_df(self, feature_sets, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        writer = HistoricalFeatureStoreWriter
        feature_set = mocker.stub("feature_set")
        feature_set.entity = "house"
        feature_set.name = "test"

        # when
        sink = Sink(
            feature_set=feature_set, writers=[writer], spark_client=spark_client
        )

        # then
        with pytest.raises(ValueError):
            sink.flush(dataframe=feature_sets)

