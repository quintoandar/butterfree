import datetime
import random
from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame

from butterfree.core.load.writers import HistoricalFeatureStoreWriter
from butterfree.testing.dataframe import assert_dataframe_equality


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
        result_df = spark_client.write_table.call_args[1]["dataframe"]

        # then
        assert_dataframe_equality(historical_feature_set_dataframe, result_df)

        assert (
            writer.db_config.format_ == spark_client.write_table.call_args[1]["format_"]
        )
        assert writer.db_config.mode == spark_client.write_table.call_args[1]["mode"]
        assert (
            writer.PARTITION_BY == spark_client.write_table.call_args[1]["partition_by"]
        )
        assert feature_set.name == spark_client.write_table.call_args[1]["table_name"]

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

    def test__create_partitions(self, spark_session, spark_context):
        # arrange
        start = datetime.datetime(year=1970, month=1, day=1)
        end = datetime.datetime(year=2020, month=12, day=31)
        random_dates = [
            (
                lambda: start
                + datetime.timedelta(
                    seconds=random.randint(0, int((end - start).total_seconds()))
                )  # noqa: S311
            )()
            .date()
            .isoformat()
            for _ in range(10000)
        ]
        data = [{"timestamp": date} for date in random_dates]
        input_df = spark_session.read.json(
            spark_context.parallelize(data, 1), schema="timestamp timestamp"
        )

        writer = HistoricalFeatureStoreWriter()

        # act
        result_df = writer._create_partitions(input_df)

        # assert
        assert result_df.select("year", "month", "day").distinct().count() == len(
            set(random_dates)
        )

    def test__repartition_df(self):
        # arrange
        writer = HistoricalFeatureStoreWriter(num_partitions=10)
        dataframe = Mock(spec=DataFrame)

        # act
        writer._repartition_df(dataframe)

        # assert
        dataframe.repartition.assert_called_with(10, *writer.PARTITION_BY)
