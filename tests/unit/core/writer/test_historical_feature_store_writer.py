import pytest

from butterfree.core.writer import HistoricalFeatureStoreWriter


class TestHistoricalFeatureStoreWriter:
    def test_write(
        self, input_feature_set_dataframe, output_feature_set_dataframe, mocker
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.write_table = mocker.stub("write_table")
        feature_set = mocker.stub("feature_set")
        writer = HistoricalFeatureStoreWriter()

        feature_set.entity = "house"
        feature_set.name = "test"

        # when
        writer.write(
            feature_set=feature_set,
            dataframe=input_feature_set_dataframe,
            spark_client=spark_client,
        )

        # then
        spark_client.write_table.assert_called_once()

        actual_df = spark_client.write_table.call_args[1]["dataframe"].collect()
        expected_df = output_feature_set_dataframe.collect()

        assert actual_df[0]["feature"] == expected_df[0]["feature"]
        assert actual_df[0]["partition__year"] == expected_df[0]["partition__year"]
        assert actual_df[0]["partition__month"] == expected_df[0]["partition__month"]
        assert actual_df[0]["partition__day"] == expected_df[0]["partition__day"]

        assert (
            writer.db_config.format_ == spark_client.write_table.call_args[1]["format_"]
        )
        assert writer.db_config.mode == spark_client.write_table.call_args[1]["mode"]
        assert (
            writer.db_config.partition_by
            == spark_client.write_table.call_args[1]["partition_by"]
        )
        assert feature_set.name == spark_client.write_table.call_args[1]["table_name"]

    def test_write_with_df_invalid(self, feature_sets, mocker):
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
                dataframe=feature_sets,
                spark_client=spark_client,
            )

    def test_validate(
        self, input_feature_set_dataframe, feature_set_count_dataframe, mocker
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.sql = mocker.stub("sql")
        spark_client.sql.return_value = feature_set_count_dataframe
        feature_set = mocker.stub("feature_set")
        feature_set.name = "test"
        query = "SELECT COUNT(1) as row FROM feature_store.test"

        writer = HistoricalFeatureStoreWriter()

        # when
        result = writer.validate(feature_set, input_feature_set_dataframe, spark_client)

        # then
        spark_client.sql.assert_called_once()

        assert query == spark_client.sql.call_args[1]["query"]
        assert result is True

    def test_validate_false(
        self, input_feature_set_dataframe, feature_set_count_dataframe, mocker
    ):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.sql = mocker.stub("sql")
        spark_client.sql.return_value = feature_set_count_dataframe.withColumn(
            "row", feature_set_count_dataframe.row + 1
        )
        feature_set = mocker.stub("feature_set")
        feature_set.name = "test"

        writer = HistoricalFeatureStoreWriter()

        # when
        result = writer.validate(feature_set, input_feature_set_dataframe, spark_client)

        # then
        assert result is False
