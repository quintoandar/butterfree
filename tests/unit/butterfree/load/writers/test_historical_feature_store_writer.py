import datetime
import random
from unittest.mock import Mock

import pytest
from pyspark.sql.functions import spark_partition_id

from butterfree.clients import SparkClient
from butterfree.load.writers import HistoricalFeatureStoreWriter
from butterfree.testing.dataframe import assert_dataframe_equality


class TestHistoricalFeatureStoreWriter:
    def test_load(
        self,
        feature_set_dataframe,
        historical_feature_set_dataframe,
        feature_set,
        mocker,
        spark_session,
    ):
        # given
        spark_client = SparkClient()
        spark_client.write_dataframe = mocker.stub("write_dataframe")
        spark_client.add_table_partitions = mocker.stub("add_table_partitions")
        spark_client.conn.conf.set(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )

        writer = HistoricalFeatureStoreWriter()

        writer.run_pre_hooks = Mock()
        writer.run_pre_hooks.return_value = feature_set_dataframe

        # when
        writer.load(
            feature_set=feature_set,
            dataframe=feature_set_dataframe,
            spark_client=spark_client,
        )

        result_df = spark_client.write_dataframe.call_args[1]["dataframe"]

        # then
        assert_dataframe_equality(historical_feature_set_dataframe, result_df)

        assert (
            writer.db_config.format_
            == spark_client.write_dataframe.call_args[1]["format_"]
        )
        assert (
            writer.db_config.mode == spark_client.write_dataframe.call_args[1]["mode"]
        )
        assert (
            writer.PARTITION_BY
            == spark_client.write_dataframe.call_args[1]["partitionBy"]
        )

    def test_write_invalid_partition_mode(
        self,
        feature_set_dataframe,
        historical_feature_set_dataframe,
        mocker,
        feature_set,
    ):
        # given
        spark_client = SparkClient()
        spark_client.write_dataframe = mocker.stub("write_dataframe")
        spark_client.conn.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

        writer = HistoricalFeatureStoreWriter()
        writer.run_pre_hooks = Mock()
        writer.run_pre_hooks.return_value = feature_set_dataframe
        # when
        with pytest.raises(RuntimeError):
            _ = writer.load(
                feature_set=feature_set,
                dataframe=feature_set_dataframe,
                spark_client=spark_client,
            )

    def test_write_in_debug_mode(
        self,
        feature_set_dataframe,
        feature_set,
        spark_session,
        historical_feature_set_dataframe,
    ):
        # given
        spark_client = SparkClient()

        writer = HistoricalFeatureStoreWriter(debug_mode=True)
        spark_session.sql("CREATE DATABASE IF NOT EXISTS {}".format(writer.database))
        spark_session.sql(
            "CREATE TABLE {}.{} (id bigint, "
            "timestamp string, "
            "feature bigint) PARTITIONED BY (year int, month int, day int)".format(
                writer.database, feature_set.name
            )
        )
        # when
        writer.load(
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
        spark_client.read = mocker.stub("read")
        spark_client.read.return_value = feature_set_dataframe

        writer = HistoricalFeatureStoreWriter()

        # when
        writer.validate(feature_set, feature_set_dataframe, spark_client)

        # then
        spark_client.read.assert_called_once()

    def test_validate_false(self, feature_set_dataframe, mocker, feature_set):
        # given
        spark_client = mocker.stub("spark_client")
        spark_client.read = mocker.stub("read")

        # limiting df to 1 row, now the counts should'n be the same
        spark_client.read.return_value = feature_set_dataframe.limit(1)

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
