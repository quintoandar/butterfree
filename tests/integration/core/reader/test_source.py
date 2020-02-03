import os

from pyspark.sql import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.reader import FileReader, KafkaReader, Source, TableReader


def create_temp_view(dataframe: DataFrame, name):
    dataframe.createOrReplaceTempView(name)


def create_db_and_table(spark, table_reader_id, table_reader_db, table_reader_table):
    spark.sql(f"create database if not exists {table_reader_db}")
    spark.sql(f"use {table_reader_db}")
    spark.sql(
        f"create table if not exists {table_reader_db}.{table_reader_table} as select * from {table_reader_id}"
    )


class TestSource:
    def test_source(self, target_df_source, target_df_table_reader, kafka_df, spark, spark_client_mock):
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
        data_sample_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_sample.parquet")

        kafka_reader_id = "c_source"
        spark_client_mock.read.return_value = kafka_df

        # when
        source_selector = Source(
            spark_client=spark_client,
            readers=[
                TableReader(
                    id=table_reader_id,
                    spark_client=spark_client,
                    database=table_reader_db,
                    table=table_reader_table,
                ),
                FileReader(
                    id=file_reader_id,
                    spark_client=spark_client,
                    path=data_sample_path,
                    format="parquet",
                ),
                KafkaReader(
                    kafka_reader_id, spark_client_mock, "host1:port,host2:port", "topic"
                ),
            ],
            query=f"select a.*, b.feature2, c.value as feature3 "
                  f"from {table_reader_id} a "
                  f"inner join {file_reader_id} b on a.id = b.id "
                  f"inner join {kafka_reader_id} c on a.id = c.key",
        )

        result_df = source_selector.construct().collect()
        target_df = target_df_source.collect()

        for i in range(0, 6):
            assert result_df[i]["id"] == target_df[i]["id"]
            assert result_df[i]["feature1"] == target_df[i]["feature1"]
            assert result_df[i]["feature2"] == target_df[i]["feature2"]
            assert result_df[i]["feature3"] == target_df[i]["feature3"]
