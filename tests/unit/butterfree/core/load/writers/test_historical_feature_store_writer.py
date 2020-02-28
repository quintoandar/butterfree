import pytest
from testing import check_dataframe_equality

from butterfree.core.load.writers import HistoricalFeatureStoreWriter


class TestHistoricalFeatureStoreWriter:
    def test_write(
        self, input_df, historical_df, mocker, feature_set,
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
        writer = HistoricalFeatureStoreWriter()

        # when
        writer.write(
            feature_set=feature_set, dataframe=input_df, spark_client=spark_client,
        )

        # then
        spark_client.write_table.assert_called_once()

        sort_columns = spark_client.write_table.call_args[1][
            "dataframe"
        ].schema.fieldNames()
        target_df = spark_client.write_table.call_args[1]["dataframe"].select(
            sort_columns
        )
        output_df = historical_df.select(sort_columns)

        assert check_dataframe_equality(output_df, target_df)
        assert (
            writer.db_config.format_ == spark_client.write_table.call_args[1]["format_"]
        )
        assert writer.db_config.mode == spark_client.write_table.call_args[1]["mode"]
        assert (
            writer.PARTITION_BY == spark_client.write_table.call_args[1]["partition_by"]
        )
        assert feature_set.name == spark_client.write_table.call_args[1]["table_name"]

    def test_write_with_df_invalid(self, not_df, mocker):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
        feature_set = mocker.stub("feature set")
        feature_set.entity = "house"
        feature_set.name = "test"

        writer = HistoricalFeatureStoreWriter()

        # then
        with pytest.raises(ValueError):
            writer.write(
                feature_set=feature_set, dataframe=not_df, spark_client=spark_client,
            )

    def test_validate(self, input_df, count_df, mocker, feature_set):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.sql = mocker.stub("sql")
        spark_client.sql.return_value = count_df

        writer = HistoricalFeatureStoreWriter()
        query_format_string = "SELECT COUNT(1) as row FROM {}.{}"
        query_count = query_format_string.format(writer.database, feature_set.name)

        # when
        result = writer.validate(feature_set, input_df, spark_client)

        # then
        spark_client.sql.assert_called_once()
        assert query_count == spark_client.sql.call_args[1]["query"]
        assert result is True

    def test_validate_false(self, input_df, count_df, mocker, feature_set):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.sql = mocker.stub("sql")
        spark_client.sql.return_value = count_df.withColumn(
            "row", count_df.row + 1
        )  # add 1 to the right dataframe count, now the counts should'n be the same

        writer = HistoricalFeatureStoreWriter()

        # when
        result = writer.validate(feature_set, input_df, spark_client)

        # then
        assert result is False
