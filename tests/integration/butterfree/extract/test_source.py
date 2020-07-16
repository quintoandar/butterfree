from typing import List

from pyspark.sql import DataFrame
from tests.integration import INPUT_PATH

from butterfree.clients import SparkClient
from butterfree.extract import Source
from butterfree.extract.readers import FileReader, TableReader


def create_temp_view(dataframe: DataFrame, name):
    dataframe.createOrReplaceTempView(name)


def create_db_and_table(spark, table_reader_id, table_reader_db, table_reader_table):
    spark.sql(f"create database if not exists {table_reader_db}")
    spark.sql(f"use {table_reader_db}")
    spark.sql(
        f"create table if not exists {table_reader_db}.{table_reader_table} "  # noqa
        f"as select * from {table_reader_id}"  # noqa
    )


def compare_dataframes(
    actual_df: DataFrame, expected_df: DataFrame, columns_sort: List[str] = None
):
    if not columns_sort:
        columns_sort = actual_df.schema.fieldNames()
    return sorted(actual_df.select(*columns_sort).collect()) == sorted(
        expected_df.select(*columns_sort).collect()
    )


class TestSource:
    def test_source(
        self, target_df_source, target_df_table_reader, spark_session,
    ):
        # given
        spark_client = SparkClient()

        table_reader_id = "a_test_source"
        table_reader_db = "db"
        table_reader_table = "table_test_source"

        create_temp_view(dataframe=target_df_table_reader, name=table_reader_id)
        create_db_and_table(
            spark=spark_session,
            table_reader_id=table_reader_id,
            table_reader_db=table_reader_db,
            table_reader_table=table_reader_table,
        )

        file_reader_id = "b_test_source"
        data_sample_path = INPUT_PATH + "/data.json"

        # when
        source = Source(
            readers=[
                TableReader(
                    id=table_reader_id,
                    database=table_reader_db,
                    table=table_reader_table,
                ),
                FileReader(id=file_reader_id, path=data_sample_path, format="json"),
            ],
            query=f"select a.*, b.feature2 "  # noqa
            f"from {table_reader_id} a "  # noqa
            f"inner join {file_reader_id} b on a.id = b.id ",  # noqa
        )

        result_df = source.construct(client=spark_client)
        target_df = target_df_source

        # then
        assert (
            compare_dataframes(
                actual_df=result_df,
                expected_df=target_df,
                columns_sort=result_df.columns,
            )
            is True
        )
