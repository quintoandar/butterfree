import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from butterfree.core.client import SparkClient
from butterfree.core.db.configs import S3Config
from butterfree.core.feature_set_pipeline import FeatureSetPipeline
from butterfree.core.reader import Source, TableReader
from butterfree.core.transform import Feature, FeatureSet
from butterfree.core.transform.aggregation import AggregatedTransform
from butterfree.core.transform.custom import CustomTransform
from butterfree.core.writer import HistoricalFeatureStoreWriter, Sink


def create_temp_view(dataframe: DataFrame, name):
    dataframe.createOrReplaceTempView(name)


def create_db_and_table(spark, table_reader_id, table_reader_db, table_reader_table):
    spark.sql(f"create database if not exists {table_reader_db}")
    spark.sql(f"use {table_reader_db}")
    spark.sql(
        f"create table if not exists {table_reader_db}.{table_reader_table} "  # noqa
        f"as select * from {table_reader_id}"  # noqa
    )


def divide(df, name, column1, column2):

    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


class TestFeatureSetPipeline:
    def test_feature_set_pipeline(self, mocked_df, spark):
        # given
        spark_client = SparkClient()

        table_reader_id = "a_source"
        table_reader_db = "db"
        table_reader_table = "table"

        create_temp_view(dataframe=mocked_df, name=table_reader_id)
        create_db_and_table(
            spark=spark,
            table_reader_id=table_reader_id,
            table_reader_db=table_reader_db,
            table_reader_table=table_reader_table,
        )

        test_pipeline = FeatureSetPipeline(
            source=Source(
                spark_client=spark_client,
                readers=[
                    TableReader(
                        id=table_reader_id,
                        spark_client=spark_client,
                        database=table_reader_db,
                        table=table_reader_table,
                    ),
                ],
                query=f"select * from {table_reader_id}",  # noqa
            ),
            feature_set=FeatureSet(
                name="feature_set",
                entity="entity",
                description="description",
                features=[
                    Feature(name="id", description="The user's Main ID or device ID",),
                    Feature(
                        name="feature1",
                        description="test",
                        transformation=AggregatedTransform(
                            aggregations=["avg", "std"],
                            partition="id",
                            windows=["2 minutes", "15 minutes"],
                        ),
                    ),
                    Feature(name="ts", description="The timestamp feature",),
                    Feature(
                        name="divided_feature",
                        description="unit test",
                        transformation=CustomTransform(
                            transformer=divide, column1="feature1", column2="feature2",
                        ),
                    ),
                ],
                key_columns=["id"],
                timestamp_column="ts",
            ),
            sink=Sink(
                writers=[
                    HistoricalFeatureStoreWriter(
                        spark_client=spark_client,
                        db_config=S3Config(
                            database="db",
                            format_="parquet",
                            path=os.path.join(
                                os.path.dirname(os.path.abspath(__file__))
                            ),
                            partition_by="ts",
                        ),
                    )
                ],
            ),
        )
        test_pipeline.run()
