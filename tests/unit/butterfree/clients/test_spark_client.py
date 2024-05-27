from datetime import datetime
from typing import Any, Optional, Union
from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from butterfree.clients import SparkClient
from butterfree.testing.dataframe import assert_dataframe_equality


def create_temp_view(dataframe: DataFrame, name: str) -> None:
    dataframe.createOrReplaceTempView(name)


def create_db_and_table(spark, database, table, view):
    spark.sql(f"create database if not exists {database}")
    spark.sql(f"use {database}")
    spark.sql(
        f"create table if not exists {database}.{table} "  # noqa
        f"as select * from {view}"  # noqa
    )


class TestSparkClient:
    def test_conn(self) -> None:
        # arrange
        spark_client = SparkClient()

        # act
        start_conn = spark_client._session

        # assert
        assert start_conn is None

    @pytest.mark.parametrize(
        "format, path, stream, schema, options",
        [
            ("parquet", ["path/to/file"], False, None, {}),
            ("csv", "path/to/file", False, None, {"header": True}),
            ("json", "path/to/file", True, None, {}),
        ],
    )
    def test_read(
        self,
        format: str,
        stream: bool,
        schema: Optional[StructType],
        path: Any,
        options: Any,
        target_df: DataFrame,
        mocked_spark_read: Mock,
    ) -> None:
        # arrange
        spark_client = SparkClient()
        mocked_spark_read.load.return_value = target_df
        spark_client._session = mocked_spark_read

        # act
        result_df = spark_client.read(
            format=format, schema=schema, stream=stream, path=path, **options
        )

        # assert
        mocked_spark_read.format.assert_called_once_with(format)
        mocked_spark_read.load.assert_called_once_with(path=path, **options)
        assert target_df.collect() == result_df.collect()

    @pytest.mark.parametrize(
        "format, path",
        [(None, "path/to/file"), ("csv", 123)],
    )
    def test_read_invalid_params(self, format: Optional[str], path: Any) -> None:
        # arrange
        spark_client = SparkClient()

        # act and assert
        with pytest.raises(ValueError):
            spark_client.read(format=format, path=path)  # type: ignore

    def test_sql(self, target_df: DataFrame) -> None:
        # arrange
        spark_client = SparkClient()
        create_temp_view(target_df, "test")

        # act
        result_df = spark_client.sql("select * from test")

        # assert
        assert result_df.collect() == target_df.collect()

    @pytest.mark.parametrize(
        "database, table, target_table_name",
        [(None, "table", "table"), ("database", "table", "database.table")],
    )
    def test_read_table(
        self,
        target_df: DataFrame,
        mocked_spark_read: Mock,
        database: Optional[str],
        table: str,
        target_table_name: str,
    ) -> None:
        # arrange
        spark_client = SparkClient()
        mocked_spark_read.table.return_value = target_df
        spark_client._session = mocked_spark_read

        # act
        result_df = spark_client.read_table(table, database)

        # assert
        mocked_spark_read.table.assert_called_once_with(target_table_name)
        assert target_df == result_df

    @pytest.mark.parametrize(
        "database, table",
        [("database", None), ("database", 123)],
    )
    def test_read_table_invalid_params(
        self, database: str, table: Optional[int]
    ) -> None:
        # arrange
        spark_client = SparkClient()

        # act and assert
        with pytest.raises(ValueError):
            spark_client.read_table(table, database)  # type: ignore

    @pytest.mark.parametrize(
        "format, mode",
        [("parquet", "append"), ("csv", "overwrite")],
    )
    def test_write_dataframe(
        self, format: str, mode: str, mocked_spark_write: Mock
    ) -> None:
        SparkClient.write_dataframe(mocked_spark_write, format, mode)
        mocked_spark_write.save.assert_called_with(format=format, mode=mode)

    @pytest.mark.parametrize(
        "format, mode",
        [(None, "append"), ("parquet", 1)],
    )
    def test_write_dataframe_invalid_params(
        self, target_df: DataFrame, format: Optional[str], mode: Union[str, int]
    ) -> None:
        # arrange
        spark_client = SparkClient()

        # act and assert
        with pytest.raises(ValueError):
            spark_client.write_dataframe(
                dataframe=target_df, format_=format, mode=mode  # type: ignore
            )

    @pytest.mark.parametrize(
        "format, mode, database, table_name, path",
        [
            ("parquet", "append", "", "test", "local/path"),
            ("csv", "overwrite", "house", "real", "s3://path"),
        ],
    )
    def test_write_table(
        self,
        format: str,
        mode: str,
        database: str,
        table_name: str,
        path: str,
        mocked_spark_write: Mock,
    ) -> None:
        # given
        name = "{}.{}".format(database, table_name)

        # when
        SparkClient.write_table(
            dataframe=mocked_spark_write,
            database=database,
            table_name=table_name,
            format_=format,
            mode=mode,
            path=path,
        )

        # then
        mocked_spark_write.saveAsTable.assert_called_with(
            mode=mode, format=format, partitionBy=None, name=name, path=path
        )

    @pytest.mark.parametrize(
        "database, table_name, path",
        [
            (None, "test", "local/path"),
            ("house", None, "s3://local/path"),
            ("user", "temp", None),
        ],
    )
    def test_write_table_with_invalid_params(
        self, database: Optional[str], table_name: Optional[str], path: Optional[str]
    ) -> None:
        df_writer = "not a spark df writer"

        with pytest.raises(ValueError):
            SparkClient.write_table(
                dataframe=df_writer,  # type: ignore
                database=database,  # type: ignore
                table_name=table_name,  # type: ignore
                path=path,  # type: ignore
            )

    def test_write_stream(self, mocked_stream_df: Mock) -> None:
        # arrange
        spark_client = SparkClient()

        processing_time = "0 seconds"
        output_mode = "update"
        checkpoint_path = "s3://path/to/checkpoint"

        # act
        stream_handler = spark_client.write_stream(
            mocked_stream_df,
            processing_time,
            output_mode,
            checkpoint_path,
            format_="parquet",
            mode="append",
        )

        # assert
        assert isinstance(stream_handler, StreamingQuery)
        mocked_stream_df.trigger.assert_called_with(processingTime=processing_time)
        mocked_stream_df.outputMode.assert_called_with(output_mode)
        mocked_stream_df.option.assert_called_with(
            "checkpointLocation", checkpoint_path
        )
        mocked_stream_df.foreachBatch.assert_called_once()
        mocked_stream_df.start.assert_called_once()

    def test_write_stream_invalid_params(self, mocked_stream_df: Mock) -> None:
        # arrange
        spark_client = SparkClient()
        mocked_stream_df.isStreaming = False

        # act and assert
        with pytest.raises(ValueError):
            spark_client.write_stream(
                mocked_stream_df,
                processing_time="0 seconds",
                output_mode="update",
                checkpoint_path="s3://path/to/checkpoint",
                format_="parquet",
                mode="append",
            )

    def test_create_temporary_view(
        self, target_df: DataFrame, spark_session: SparkSession
    ) -> None:
        # arrange
        spark_client = SparkClient()

        # act
        spark_client.create_temporary_view(target_df, "temp_view")
        result_df = spark_session.table("temp_view")

        # assert
        assert_dataframe_equality(target_df, result_df)

    def test_add_table_partitions(self, mock_spark_sql: Mock):
        # arrange
        target_command = (
            f"ALTER TABLE `db`.`table` ADD IF NOT EXISTS "
            f"PARTITION ( year = 2020, month = 8, day = 14 ) "
            f"PARTITION ( year = 2020, month = 8, day = 15 ) "
            f"PARTITION ( year = 2020, month = 8, day = 16 )"
        )

        spark_client = SparkClient()
        spark_client._session = mock_spark_sql
        partitions = [
            {"year": 2020, "month": 8, "day": 14},
            {"year": 2020, "month": 8, "day": 15},
            {"year": 2020, "month": 8, "day": 16},
        ]

        # act
        spark_client.add_table_partitions(partitions, "table", "db")

        # assert
        mock_spark_sql.assert_called_once_with(target_command)

    @pytest.mark.parametrize(
        "partition",
        [
            [{"float_partition": 2.72}],
            [{123: 2020}],
            [{"date": datetime(year=2020, month=8, day=18)}],
        ],
    )
    def test_add_invalid_partitions(self, mock_spark_sql: Mock, partition):
        # arrange
        spark_client = SparkClient()
        spark_client._session = mock_spark_sql

        # act and assert
        with pytest.raises(ValueError):
            spark_client.add_table_partitions(partition, "table", "db")

    def test_get_schema(
        self, target_df: DataFrame, spark_session: SparkSession
    ) -> None:
        # arrange
        spark_client = SparkClient()
        create_temp_view(dataframe=target_df, name="temp_view")
        create_db_and_table(
            spark=spark_session,
            database="test_db",
            table="test_table",
            view="temp_view",
        )

        expected_schema = [
            {"col_name": "col1", "data_type": "string"},
            {"col_name": "col2", "data_type": "bigint"},
        ]

        # act
        schema = spark_client.get_schema(table="test_table", database="test_db")

        # assert
        assert schema, expected_schema
