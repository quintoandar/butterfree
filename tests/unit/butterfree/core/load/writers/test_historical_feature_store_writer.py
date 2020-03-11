import pytest

from butterfree.core.load.writers import HistoricalFeatureStoreWriter


class TestHistoricalFeatureStoreWriter:
    def test_write(
        self,
        feature_set_dataframe,
        historical_feature_set_dataframe,
        mocker,
        feature_set,
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
        writer = HistoricalFeatureStoreWriter()

        # when
        writer.write(
            feature_set=feature_set,
            dataframe=feature_set_dataframe,
            spark_client=spark_client,
        )

        # then
        spark_client.write_table.assert_called_once()

        sort_columns = spark_client.write_table.call_args[1][
            "dataframe"
        ].schema.fieldNames()
        actual_df = (
            spark_client.write_table.call_args[1]["dataframe"]
            .select(sort_columns)
            .collect()
        )
        output_feature_set_dataframe = historical_feature_set_dataframe.select(
            sort_columns
        ).collect()

        assert sorted(output_feature_set_dataframe) == sorted(actual_df)
        assert (
            writer.db_config.format_ == spark_client.write_table.call_args[1]["format_"]
        )
        assert writer.db_config.mode == spark_client.write_table.call_args[1]["mode"]
        assert (
            writer.PARTITION_BY == spark_client.write_table.call_args[1]["partition_by"]
        )
        assert feature_set.name == spark_client.write_table.call_args[1]["table_name"]

    def test_write_with_df_invalid(self, not_feature_set_dataframe, mocker):
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
                feature_set=feature_set,
                dataframe=not_feature_set_dataframe,
                spark_client=spark_client,
            )

    def test_validate(
        self, feature_set_dataframe, count_feature_set_dataframe, mocker, feature_set
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.sql = mocker.stub("sql")
        spark_client.sql.return_value = count_feature_set_dataframe

        writer = HistoricalFeatureStoreWriter()
        query_format_string = "SELECT COUNT(1) as row FROM {}.{}"
        query_count = query_format_string.format(writer.database, feature_set.name)

        # when
        result = writer.validate(feature_set, feature_set_dataframe, spark_client)

        # then
        spark_client.sql.assert_called_once()
        assert query_count == spark_client.sql.call_args[1]["query"]
        assert result is None

    def test_validate_false(
        self, feature_set_dataframe, count_feature_set_dataframe, mocker, feature_set
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.sql = mocker.stub("sql")
        spark_client.sql.return_value = count_feature_set_dataframe.withColumn(
            "row", count_feature_set_dataframe.row + 1
        )  # add 1 to the right dataframe count, now the counts should'n be the same

        writer = HistoricalFeatureStoreWriter()

        # when
        with pytest.raises(AssertionError):
            _ = writer.validate(feature_set, feature_set_dataframe, spark_client)
