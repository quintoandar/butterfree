import shutil
from unittest.mock import Mock

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from butterfree.clients import SparkClient
from butterfree.configs import environment
from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.extract import Source
from butterfree.extract.readers import TableReader
from butterfree.hooks import Hook
from butterfree.load import Sink
from butterfree.load.writers import HistoricalFeatureStoreWriter
from butterfree.pipelines.feature_set_pipeline import FeatureSetPipeline
from butterfree.testing.dataframe import assert_dataframe_equality
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import (
    CustomTransform,
    SparkFunctionTransform,
    SQLExpressionTransform,
)
from butterfree.transform.utils import Function


class AddHook(Hook):
    def __init__(self, value):
        self.value = value

    def run(self, dataframe):
        return dataframe.withColumn("feature", F.expr(f"feature + {self.value}"))


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
        self,
        mocked_df,
        spark_session,
        fixed_windows_output_feature_set_dataframe,
        mocker,
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

        spark_client = SparkClient()
        spark_client.conn.conf.set(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )

        dbconfig = Mock()
        dbconfig.mode = "overwrite"
        dbconfig.format_ = "parquet"
        dbconfig.get_options = Mock(
            return_value={"path": "test_folder/historical/entity/feature_set"}
        )
        dbconfig.get_path_with_partitions = Mock(
            return_value=[
                "test_folder/historical/entity/feature_set/year=2016/month=4/day=11"
            ]
        )

        historical_writer = HistoricalFeatureStoreWriter(
            db_config=dbconfig, interval_mode=True
        )

        historical_writer.check_schema_hook = mocker.stub("check_schema_hook")
        historical_writer.check_schema_hook.run = mocker.stub("run")
        historical_writer.check_schema_hook.run.return_value = (
            fixed_windows_output_feature_set_dataframe
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
            sink=Sink(writers=[historical_writer],),
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

    def test_feature_set_pipeline_with_dates(
        self,
        mocked_date_df,
        spark_session,
        fixed_windows_output_feature_set_date_dataframe,
        feature_set_pipeline,
        mocker,
    ):
        # arrange
        table_reader_table = "b_table"
        create_temp_view(dataframe=mocked_date_df, name=table_reader_table)

        historical_writer = HistoricalFeatureStoreWriter(debug_mode=True)

        historical_writer.check_schema_hook = mocker.stub("check_schema_hook")
        historical_writer.check_schema_hook.run = mocker.stub("run")
        historical_writer.check_schema_hook.run.return_value = (
            fixed_windows_output_feature_set_date_dataframe
        )

        feature_set_pipeline.sink.writers = [historical_writer]

        # act
        feature_set_pipeline.run(start_date="2016-04-12", end_date="2016-04-13")

        df = spark_session.sql("select * from historical_feature_store__feature_set")

        # assert
        assert_dataframe_equality(df, fixed_windows_output_feature_set_date_dataframe)

    def test_feature_set_pipeline_with_execution_date(
        self,
        mocked_date_df,
        spark_session,
        fixed_windows_output_feature_set_date_dataframe,
        feature_set_pipeline,
        mocker,
    ):
        # arrange
        table_reader_table = "b_table"
        create_temp_view(dataframe=mocked_date_df, name=table_reader_table)

        target_df = fixed_windows_output_feature_set_date_dataframe.filter(
            "timestamp < '2016-04-13'"
        )

        historical_writer = HistoricalFeatureStoreWriter(debug_mode=True)

        historical_writer.check_schema_hook = mocker.stub("check_schema_hook")
        historical_writer.check_schema_hook.run = mocker.stub("run")
        historical_writer.check_schema_hook.run.return_value = target_df

        feature_set_pipeline.sink.writers = [historical_writer]

        # act
        feature_set_pipeline.run_for_date(execution_date="2016-04-12")

        df = spark_session.sql("select * from historical_feature_store__feature_set")

        # assert
        assert_dataframe_equality(df, target_df)

    def test_pipeline_with_hooks(self, spark_session, mocked_df):
        # arrange
        hook1 = AddHook(value=1)

        spark_session.sql(
            "select 1 as id, timestamp('2020-01-01') as timestamp, 0 as feature"
        ).createOrReplaceTempView("test")

        test_pipeline = FeatureSetPipeline(
            source=Source(
                readers=[TableReader(id="reader", table="test",).add_post_hook(hook1)],
                query="select * from reader",
            ).add_post_hook(hook1),
            feature_set=FeatureSet(
                name="feature_set",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature",
                        description="test",
                        transformation=SQLExpressionTransform(expression="feature + 1"),
                        dtype=DataType.INTEGER,
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
            )
            .add_pre_hook(hook1)
            .add_post_hook(hook1),
            sink=Sink(
                writers=[
                    HistoricalFeatureStoreWriter(
                        debug_mode=True
                    )  # .add_pre_hook(hook1)
                ],
            ).add_pre_hook(hook1),
        )

        target_df = spark_session.sql(
            "select 1 as id, timestamp('2020-01-01') as timestamp, 6 as feature, 2020"
            "as year, 1 as month, 1 as day"
        )

        # act
        test_pipeline.run()
        output_df = spark_session.table("historical_feature_store__feature_set")

        # assert
        output_df.show()
        assert_dataframe_equality(output_df, target_df)
