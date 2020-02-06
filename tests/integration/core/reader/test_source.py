import os

from pyspark.sql import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.reader import FileReader, Source, TableReader


def create_temp_view(dataframe: DataFrame, name):
    dataframe.createOrReplaceTempView(name)


def create_db_and_table(spark, table_reader_id, table_reader_db, table_reader_table):
    spark.sql(f"create database if not exists {table_reader_db}")
    spark.sql(f"use {table_reader_db}")
    spark.sql(
        f"create table if not exists {table_reader_db}.{table_reader_table} "  # noqa
        f"as select * from {table_reader_id}"  # noqa
    )


class TestSource:
    def test_source(
        self, target_df_source, target_df_table_reader, spark,
    ):
        # given
        spark_client = SparkClient()

        table_reader_id = "a_source"
        table_reader_db = "db"
        table_reader_table = "table"

        create_temp_view(dataframe=target_df_table_reader, name=table_reader_id)
        create_db_and_table(
            spark=spark,
            table_reader_id=table_reader_id,
            table_reader_db=table_reader_db,
            table_reader_table=table_reader_table,
        )

        file_reader_id = "b_source"
        data_sample_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data_sample.json/data.json"
        )

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

        result_df = source.construct(client=spark_client).collect()
        target_df = target_df_source.collect()

        # then
        for i in range(0, 6):
            assert result_df[i]["id"] == target_df[i]["id"]
            assert result_df[i]["feature1"] == target_df[i]["feature1"]
            assert result_df[i]["feature2"] == target_df[i]["feature2"]
