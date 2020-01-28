import pytest
from pyspark.sql.dataframe import DataFrame
from tests.unit.core.writer.conftest import feature_set_empty, feature_set_without_ts

from butterfree.core.writer import HistoricalFeatureStoreWriter


def create_temp_view(dataframe: DataFrame, name):
    dataframe.createOrReplaceTempView(name)


class TestHistoricalFeatureStoreWriter:
    def test_write(self, feature_set_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
        feature_set = mocker.stub("feature_set")
        writer = HistoricalFeatureStoreWriter(spark_client)

        feature_set.entity = "house"
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

    @pytest.mark.parametrize(
        "dataframe",
        [feature_set_empty, feature_set_without_ts, "not a spark df writer"],
    )
    def test_write_with_df_invalid(self, dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
        feature_set = mocker.stub("feature set")
        feature_set.entity = "house"
        feature_set.name = "test"

        writer = HistoricalFeatureStoreWriter(spark_client)

        # then
        with pytest.raises(ValueError):
            assert writer.write(feature_set=feature_set, dataframe=dataframe)

    def test_validate(self, feature_set_dataframe, feature_set_count_dataframe, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.sql = mocker.stub("sql")
        spark_client.sql.return_value = feature_set_count_dataframe
        feature_set = mocker.stub("feature_set")
        feature_set.name = "test"
        query = "SELECT COUNT(1) as row FROM feature_store.test"

        writer = HistoricalFeatureStoreWriter(spark_client)

        # when
        result = writer.validate(feature_set, feature_set_dataframe)

        # then
        spark_client.sql.assert_called_once()

        assert query == spark_client.sql.call_args[1]["query"]
        assert result is True

    def test_validate_false(
        self, feature_set_dataframe, feature_set_count_dataframe, mocker
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.sql = mocker.stub("sql")
        spark_client.sql.return_value = feature_set_count_dataframe.withColumn(
            "row", feature_set_count_dataframe.row + 1
        )
        feature_set = mocker.stub("feature_set")
        feature_set.name = "test"

        writer = HistoricalFeatureStoreWriter(spark_client)

        # when
        result = writer.validate(feature_set, feature_set_dataframe)

        # then
        assert result is False
