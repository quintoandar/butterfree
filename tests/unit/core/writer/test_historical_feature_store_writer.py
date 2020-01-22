import pytest

from butterfree.core.writer import HistoricalFeatureStoreWriter


class TestHistoricalFeatureStoreWriter:
    def test_write(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
        writer = HistoricalFeatureStoreWriter(spark_client)
        table_name = "test"

        # when
        writer.write(dataframe=feature_set_dataframe, name=table_name)

        # then
        spark_client.write_table.assert_called_once()

        assert sorted(feature_set_dataframe.collect()) == sorted(
            spark_client.write_table.call_args[1]["dataframe"].collect()
        )
        assert writer.DEFAULT_FORMAT == spark_client.write_table.call_args[1]["format_"]
        assert writer.DEFAULT_MODE == spark_client.write_table.call_args[1]["mode"]
        assert (
            writer.DEFAULT_PARTITION_BY
            == spark_client.write_table.call_args[1]["partition_by"]
        )
        assert table_name == spark_client.write_table.call_args[1]["table_name"]

    def test_write_with_df_invalid(
        self, feature_set_empty, feature_set_without_ts, mocker
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")

        writer = HistoricalFeatureStoreWriter(spark_client)
        table_name = "test"

        # then
        with pytest.raises(ValueError):
            assert writer.write(dataframe=feature_set_empty, name=table_name)

        with pytest.raises(ValueError):
            assert writer.write(dataframe=feature_set_without_ts, name=table_name)

    def test_validate(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read = mocker.stub("read")
        mock_path = mocker.stub("path")

        format_ = "parquet"
        writer = HistoricalFeatureStoreWriter(spark_client)

        # when
        writer.validate(feature_set_dataframe, format_, mock_path)

        # then
        spark_client.read.assert_called_once()

    @pytest.mark.parametrize(
        "format_, path", [(None, "path/table"), ("parquet", None), (1, 123)],
    )
    def test_validate_invalid_params(self, feature_set_dataframe, format_, path, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read = mocker.stub("read")

        writer = HistoricalFeatureStoreWriter(spark_client)

        # then
        with pytest.raises(ValueError):
            writer.validate(feature_set_dataframe, format_, path)
