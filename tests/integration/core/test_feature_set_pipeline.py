import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.db.configs import S3Config
from butterfree.core.feature_set_pipeline import FeatureSetPipeline
from butterfree.core.reader import Source, TableReader
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import (
    AggregatedTransform,
    CustomTransform,
)
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


def divide(df, fs, column1, column2):
    name = fs.get_output_columns()[0]
    df = df.withColumn(name, F.col(column1) / F.col(column2))
    return df


class TestFeatureSetPipeline:
    def test_feature_set_pipeline(self, mocked_df, spark):
        # given

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
                readers=[
                    TableReader(
                        id=table_reader_id,
                        database=table_reader_db,
                        table=table_reader_table,
                    ),
                ],
                query=f"select * from {table_reader_id} ",  # noqa
            ),
            feature_set=FeatureSet(
                name="feature_set",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature1",
                        description="test",
                        transformation=AggregatedTransform(
                            aggregations=["avg", "stddev_pop"],
                            partition="id",
                            windows=["2 minutes", "15 minutes"],
                            mode=["fixed_windows"],
                        ),
                    ),
                    Feature(
                        name="divided_feature",
                        description="unit test",
                        transformation=CustomTransform(
                            transformer=divide, column1="feature1", column2="feature2",
                        ),
                    ),
                ],
                keys=[
                    KeyFeature(name="id", description="The user's Main ID or device ID")
                ],
                timestamp=TimestampFeature(),
            ),
            sink=Sink(
                writers=[
                    HistoricalFeatureStoreWriter(
                        db_config=S3Config(
                            database="db",
                            format_="parquet",
                            path=os.path.join(
                                os.path.dirname(os.path.abspath(__file__))
                            ),
                        ),
                    )
                ],
            ),
        )
        test_pipeline.run()

        df = (
            spark.read.parquet(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "historical/entity/feature_set",
                )
            )
            .orderBy(TIMESTAMP_COLUMN)
            .collect()
        )

        assert df[0]["feature1__avg_over_2_minutes_fixed_windows"] == 200
        assert df[1]["feature1__avg_over_2_minutes_fixed_windows"] == 300
        assert df[2]["feature1__avg_over_2_minutes_fixed_windows"] == 400
        assert df[3]["feature1__avg_over_2_minutes_fixed_windows"] == 500
        assert df[0]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[1]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[2]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[3]["feature1__stddev_pop_over_2_minutes_fixed_windows"] == 0
        assert df[0]["feature1__avg_over_15_minutes_fixed_windows"] == 200
        assert df[1]["feature1__avg_over_15_minutes_fixed_windows"] == 250
        assert df[2]["feature1__avg_over_15_minutes_fixed_windows"] == 350
        assert df[3]["feature1__avg_over_15_minutes_fixed_windows"] == 500
        assert df[0]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 0
        assert df[1]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 50
        assert df[2]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 50
        assert df[3]["feature1__stddev_pop_over_15_minutes_fixed_windows"] == 0
        assert df[0]["divided_feature"] == 1
        assert df[1]["divided_feature"] == 1
        assert df[2]["divided_feature"] == 1
        assert df[3]["divided_feature"] == 1
