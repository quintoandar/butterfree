import pytest

# from butterfree.core.transform import FeatureSet
from butterfree.core.writer import HistoricalFeatureStoreWriter


class TestHistoricalFeatureStoreWriter:
    def test_write(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
        feature_set = mocker.stub("feature_set")
        writer = HistoricalFeatureStoreWriter(spark_client)

        feature_set.name = "test"

        # when
        writer.write(feature_set=feature_set, dataframe=feature_set_dataframe)

        # then
        spark_client.write_table.assert_called_once()

        assert sorted(feature_set_dataframe.collect()) == sorted(
            spark_client.write_table.call_args[1]["dataframe"].collect()
        )
        assert (
            writer.db_config.format_ == spark_client.write_table.call_args[1]["format_"]
        )
        assert writer.db_config.mode == spark_client.write_table.call_args[1]["mode"]
        assert (
            writer.db_config.partition_by
            == spark_client.write_table.call_args[1]["partition_by"]
        )
        assert feature_set.name == spark_client.write_table.call_args[1]["table_name"]

    def test_write_with_df_invalid(
        self, feature_set_empty, feature_set_without_ts, mocker
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
        feature_set = mocker.stub("feature set")

        writer = HistoricalFeatureStoreWriter(spark_client)
        feature_set.name = "test"
        df_writer = "not a spark df writer"

        # then
        with pytest.raises(ValueError):
            assert writer.write(feature_set=feature_set, dataframe=feature_set_empty)

        with pytest.raises(ValueError):
            assert writer.write(
                feature_set=feature_set, dataframe=feature_set_without_ts
            )

        with pytest.raises(ValueError):
            assert writer.write(feature_set=feature_set, dataframe=df_writer)

    def test_validate(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read = mocker.stub("read")
        feature_set = mocker.stub("feature_set")
        feature_set.name = "test"

        writer = HistoricalFeatureStoreWriter(spark_client)

        # when
        writer.validate(feature_set, feature_set_dataframe)

        # then
        spark_client.read.assert_called_once()

    @pytest.mark.parametrize(
        "format_, path", [(None, "path/table"), ("parquet", None), (1, 123)],
    )
    def test_validate_invalid_params(
        self, feature_set_dataframe, format_, path, mocker
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read = mocker.stub("read")
        feature_set = mocker.stub("feature_set")
        feature_set.name = "test"
        db_config = mocker.stub("db_config")
        db_config.format_ = format
        db_config.path = path

        writer = HistoricalFeatureStoreWriter(spark_client, db_config)

        # then
        with pytest.raises(ValueError):
            writer.validate(feature_set, feature_set_dataframe)
