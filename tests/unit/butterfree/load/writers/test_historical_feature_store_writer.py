import datetime
import random

import pytest
from pyspark.sql.functions import spark_partition_id

from butterfree.clients import SparkClient
from butterfree.load.writers import HistoricalFeatureStoreWriter
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

    def test_write_in_debug_mode(
        self,
        feature_set_dataframe,
        historical_feature_set_dataframe,
        feature_set,
        spark_session,
    ):
        # given
        spark_client = SparkClient()
        writer = HistoricalFeatureStoreWriter(debug_mode=True)

        # when
        writer.write(
            feature_set=feature_set,
            dataframe=feature_set_dataframe,
            spark_client=spark_client,
        )
        result_df = spark_session.table(f"historical_feature_store__{feature_set.name}")

        # then
        assert_dataframe_equality(historical_feature_set_dataframe, result_df)

    def test_validate(self, feature_set_dataframe, mocker, feature_set):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read_table = mocker.stub("read_table")
        spark_client.read_table.return_value = feature_set_dataframe

        writer = HistoricalFeatureStoreWriter()

        # when
        writer.validate(feature_set, feature_set_dataframe, spark_client)

        # then
        spark_client.read_table.assert_called_once()

    def test_validate_false(self, feature_set_dataframe, mocker, feature_set):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read_table = mocker.stub("read_table")

        # limiting df to 1 row, now the counts should'n be the same
        spark_client.read_table.return_value = feature_set_dataframe.limit(1)

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
                    seconds=random.randint(  # noqa: S311
                        0, int((end - start).total_seconds())
                    )
                )
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

    def test__repartition_df(self, spark_session, spark_context):
        # arrange
        start = datetime.datetime(year=1970, month=1, day=1)
        end = datetime.datetime(year=2020, month=12, day=31)
        random_dates = [
            (
                lambda: start
                + datetime.timedelta(
                    seconds=random.randint(  # noqa: S311
                        0, int((end - start).total_seconds())
                    )
                )
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
        # Only one partition id, meaning data is not partitioned
        assert input_df.select(spark_partition_id()).distinct().count() == 1
        # Desired number of partitions
        assert result_df.select(spark_partition_id()).distinct().count() == 200

    @pytest.mark.parametrize(
        "written_count, dataframe_count, threshold",
        [(100, 101, None), (100, 99, None), (100, 108, 0.10), (100, 92, 0.10)],
    )
    def test__assert_validation_count(self, written_count, dataframe_count, threshold):
        # arrange
        writer = (
            HistoricalFeatureStoreWriter(validation_threshold=threshold)
            if threshold
            else HistoricalFeatureStoreWriter()
        )

        # act and assert
        writer._assert_validation_count("table", written_count, dataframe_count)

    @pytest.mark.parametrize(
        "written_count, dataframe_count, threshold",
        [(100, 102, None), (100, 98, None), (100, 111, 0.10), (100, 88, 0.10)],
    )
    def test__assert_validation_count_error(
        self, written_count, dataframe_count, threshold
    ):
        # arrange
        writer = (
            HistoricalFeatureStoreWriter(validation_threshold=threshold)
            if threshold
            else HistoricalFeatureStoreWriter()
        )

        # act and assert
        with pytest.raises(AssertionError):
            writer._assert_validation_count("table", written_count, dataframe_count)
