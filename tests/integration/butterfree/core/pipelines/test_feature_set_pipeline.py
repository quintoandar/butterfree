import shutil
from unittest.mock import Mock

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from butterfree.core.configs import environment
from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
from butterfree.core.extract import Source
from butterfree.core.extract.readers import TableReader
from butterfree.core.load import Sink
from butterfree.core.load.writers import HistoricalFeatureStoreWriter
from butterfree.core.pipelines.feature_set_pipeline import FeatureSetPipeline
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import (
    CustomTransform,
    SparkFunctionTransform,
)
from butterfree.core.transform.utils.function import Function
from butterfree.testing.dataframe import assert_dataframe_equality


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
    def test_feature_set_pipeline(
        self, mocked_df, spark_session, fixed_windows_output_feature_set_dataframe
    ):
        # arrange
        table_reader_id = "a_source"
        table_reader_table = "table"
        table_reader_db = environment.get_variable("FEATURE_STORE_HISTORICAL_DATABASE")
        create_temp_view(dataframe=mocked_df, name=table_reader_id)
        create_db_and_table(
            spark=spark_session,
            table_reader_id=table_reader_id,
            table_reader_db=table_reader_db,
            table_reader_table=table_reader_table,
        )
        dbconfig = Mock()
        dbconfig.get_options = Mock(
            return_value={
                "mode": "overwrite",
                "format_": "parquet",
                "path": "test_folder/historical/entity/feature_set",
            }
        )

        # act
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
                        transformation=SparkFunctionTransform(
                            functions=[
                                Function(F.avg, DataType.FLOAT),
                                Function(F.stddev_pop, DataType.FLOAT),
                            ],
                        ).with_window(
                            partition_by="id",
                            order_by=TIMESTAMP_COLUMN,
                            mode="fixed_windows",
                            window_definition=["2 minutes", "15 minutes"],
                        ),
                    ),
                    Feature(
                        name="divided_feature",
                        description="unit test",
                        dtype=DataType.FLOAT,
                        transformation=CustomTransform(
                            transformer=divide, column1="feature1", column2="feature2",
                        ),
                    ),
                ],
                keys=[
                    KeyFeature(
                        name="id",
                        description="The user's Main ID or device ID",
                        dtype=DataType.INTEGER,
                    )
                ],
                timestamp=TimestampFeature(),
            ),
            sink=Sink(writers=[HistoricalFeatureStoreWriter(db_config=dbconfig)],),
        )
        test_pipeline.run()

        # assert
        path = dbconfig.get_options("historical/entity/feature_set").get("path")
        df = spark_session.read.parquet(path).orderBy(TIMESTAMP_COLUMN)

        target_df = fixed_windows_output_feature_set_dataframe.orderBy(
            test_pipeline.feature_set.timestamp_column
        )

        # assert
        assert_dataframe_equality(df, target_df)

        # tear down
        shutil.rmtree("test_folder")
