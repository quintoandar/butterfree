import shutil
from unittest.mock import Mock

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from butterfree.configs import environment
from butterfree.configs.db import MetastoreConfig
from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.dataframe_service.incremental_strategy import IncrementalStrategy
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


class RunHook(Hook):
    def __init__(self, id):
        self.id = id

    def run(self, dataframe):
        return dataframe.withColumn(
            "run_id",
            F.when(F.lit(self.id).isNotNull(), F.lit(self.id)).otherwise(F.lit(None)),
        )


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


def create_ymd(dataframe):
    return (
        dataframe.withColumn("year", F.year(F.col("timestamp")))
        .withColumn("month", F.month(F.col("timestamp")))
        .withColumn("day", F.dayofmonth(F.col("timestamp")))
    )


class TestFeatureSetPipeline:
    def test_feature_set_pipeline(
        self, mocked_df, spark_session, fixed_windows_output_feature_set_dataframe,
    ):
        # arrange
        table_reader_id = "a_source"
        table_reader_table = "table"
        table_reader_db = environment.get_variable("FEATURE_STORE_HISTORICAL_DATABASE")

        path = "test_folder/historical/entity/feature_set"

        dbconfig = MetastoreConfig()
        dbconfig.get_options = Mock(
            return_value={"mode": "overwrite", "format_": "parquet", "path": path}
        )

        historical_writer = HistoricalFeatureStoreWriter(
            db_config=dbconfig, debug_mode=True
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
            sink=Sink(writers=[historical_writer]),
        )
        test_pipeline.run()

        # act and assert
        dbconfig.get_path_with_partitions = Mock(
            return_value=["historical/entity/feature_set",]
        )

        # assert
        df = spark_session.sql("select * from historical_feature_store__feature_set")

        target_df = fixed_windows_output_feature_set_dataframe.orderBy(
            test_pipeline.feature_set.timestamp_column
        )

        # assert
        assert_dataframe_equality(df, target_df)

    def test_feature_set_pipeline_with_dates(
        self,
        mocked_date_df,
        spark_session,
        fixed_windows_output_feature_set_date_dataframe,
        feature_set_pipeline,
    ):
        # arrange
        table_reader_table = "b_table"
        create_temp_view(dataframe=mocked_date_df, name=table_reader_table)

        historical_writer = HistoricalFeatureStoreWriter(debug_mode=True)

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
    ):
        # arrange
        table_reader_table = "b_table"
        create_temp_view(dataframe=mocked_date_df, name=table_reader_table)

        target_df = fixed_windows_output_feature_set_date_dataframe.filter(
            "timestamp < '2016-04-13'"
        )

        historical_writer = HistoricalFeatureStoreWriter(debug_mode=True)

        feature_set_pipeline.sink.writers = [historical_writer]

        # act
        feature_set_pipeline.run_for_date(execution_date="2016-04-12")

        df = spark_session.sql("select * from historical_feature_store__feature_set")

        # assert
        assert_dataframe_equality(df, target_df)

    def test_pipeline_with_hooks(self, spark_session):
        # arrange
        hook1 = AddHook(value=1)

        spark_session.sql(
            "select 1 as id, timestamp('2020-01-01') as timestamp, 0 as feature"
        ).createOrReplaceTempView("test")

        target_df = spark_session.sql(
            "select 1 as id, timestamp('2020-01-01') as timestamp, 6 as feature, 2020 "
            "as year, 1 as month, 1 as day"
        )

        historical_writer = HistoricalFeatureStoreWriter(debug_mode=True)

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
            sink=Sink(writers=[historical_writer],).add_pre_hook(hook1),
        )

        # act
        test_pipeline.run()
        output_df = spark_session.table("historical_feature_store__feature_set")

        # assert
        output_df.show()
        assert_dataframe_equality(output_df, target_df)

    def test_pipeline_interval_run(
        self, mocked_date_df, pipeline_interval_run_target_dfs, spark_session
    ):
        """Testing pipeline's idempotent interval run feature.
        Source data:
        +-------+---+-------------------+-------------------+
        |feature| id|                 ts|          timestamp|
        +-------+---+-------------------+-------------------+
        |    200|  1|2016-04-11 11:31:11|2016-04-11 11:31:11|
        |    300|  1|2016-04-12 11:44:12|2016-04-12 11:44:12|
        |    400|  1|2016-04-13 11:46:24|2016-04-13 11:46:24|
        |    500|  1|2016-04-14 12:03:21|2016-04-14 12:03:21|
        +-------+---+-------------------+-------------------+
        The test executes 3 runs for different time intervals. The input data has 4 data
        points: 2016-04-11, 2016-04-12, 2016-04-13 and 2016-04-14. The following run
        specifications are:
        1)  Interval: from 2016-04-11 to 2016-04-13
            Target table result:
            +---+-------+---+-----+------+-------------------+----+
            |day|feature| id|month|run_id|          timestamp|year|
            +---+-------+---+-----+------+-------------------+----+
            | 11|    200|  1|    4|     1|2016-04-11 11:31:11|2016|
            | 12|    300|  1|    4|     1|2016-04-12 11:44:12|2016|
            | 13|    400|  1|    4|     1|2016-04-13 11:46:24|2016|
            +---+-------+---+-----+------+-------------------+----+
        2)  Interval: only 2016-04-14.
            Target table result:
            +---+-------+---+-----+------+-------------------+----+
            |day|feature| id|month|run_id|          timestamp|year|
            +---+-------+---+-----+------+-------------------+----+
            | 11|    200|  1|    4|     1|2016-04-11 11:31:11|2016|
            | 12|    300|  1|    4|     1|2016-04-12 11:44:12|2016|
            | 13|    400|  1|    4|     1|2016-04-13 11:46:24|2016|
            | 14|    500|  1|    4|     2|2016-04-14 12:03:21|2016|
            +---+-------+---+-----+------+-------------------+----+
        3)  Interval: only 2016-04-11.
            Target table result:
            +---+-------+---+-----+------+-------------------+----+
            |day|feature| id|month|run_id|          timestamp|year|
            +---+-------+---+-----+------+-------------------+----+
            | 11|    200|  1|    4|     3|2016-04-11 11:31:11|2016|
            | 12|    300|  1|    4|     1|2016-04-12 11:44:12|2016|
            | 13|    400|  1|    4|     1|2016-04-13 11:46:24|2016|
            | 14|    500|  1|    4|     2|2016-04-14 12:03:21|2016|
            +---+-------+---+-----+------+-------------------+----+
        """
        # arrange
        create_temp_view(dataframe=mocked_date_df, name="input_data")

        db = environment.get_variable("FEATURE_STORE_HISTORICAL_DATABASE")
        path = "test_folder/historical/entity/feature_set"

        spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark_session.sql(f"create database if not exists {db}")
        spark_session.sql(
            f"create table if not exists {db}.feature_set_interval "
            f"(id int, timestamp timestamp, feature int, "
            f"run_id int, year int, month int, day int);"
        )

        dbconfig = MetastoreConfig()
        dbconfig.get_options = Mock(
            return_value={"mode": "overwrite", "format_": "parquet", "path": path}
        )

        historical_writer = HistoricalFeatureStoreWriter(
            db_config=dbconfig, interval_mode=True
        )

        first_run_hook = RunHook(id=1)
        second_run_hook = RunHook(id=2)
        third_run_hook = RunHook(id=3)

        (
            first_run_target_df,
            second_run_target_df,
            third_run_target_df,
        ) = pipeline_interval_run_target_dfs

        test_pipeline = FeatureSetPipeline(
            source=Source(
                readers=[
                    TableReader(id="id", table="input_data",).with_incremental_strategy(
                        IncrementalStrategy("ts")
                    ),
                ],
                query="select * from id ",
            ),
            feature_set=FeatureSet(
                name="feature_set_interval",
                entity="entity",
                description="",
                keys=[KeyFeature(name="id", description="", dtype=DataType.INTEGER,)],
                timestamp=TimestampFeature(from_column="ts"),
                features=[
                    Feature(name="feature", description="", dtype=DataType.INTEGER),
                    Feature(name="run_id", description="", dtype=DataType.INTEGER),
                ],
            ),
            sink=Sink([historical_writer],),
        )

        # act and assert
        dbconfig.get_path_with_partitions = Mock(
            return_value=[
                "test_folder/historical/entity/feature_set/year=2016/month=4/day=11",
                "test_folder/historical/entity/feature_set/year=2016/month=4/day=12",
                "test_folder/historical/entity/feature_set/year=2016/month=4/day=13",
            ]
        )
        test_pipeline.feature_set.add_pre_hook(first_run_hook)
        test_pipeline.run(end_date="2016-04-13", start_date="2016-04-11")
        first_run_output_df = spark_session.read.parquet(path)
        assert_dataframe_equality(first_run_output_df, first_run_target_df)

        dbconfig.get_path_with_partitions = Mock(
            return_value=[
                "test_folder/historical/entity/feature_set/year=2016/month=4/day=14",
            ]
        )
        test_pipeline.feature_set.add_pre_hook(second_run_hook)
        test_pipeline.run_for_date("2016-04-14")
        second_run_output_df = spark_session.read.parquet(path)
        assert_dataframe_equality(second_run_output_df, second_run_target_df)

        dbconfig.get_path_with_partitions = Mock(
            return_value=[
                "test_folder/historical/entity/feature_set/year=2016/month=4/day=11",
            ]
        )
        test_pipeline.feature_set.add_pre_hook(third_run_hook)
        test_pipeline.run_for_date("2016-04-11")
        third_run_output_df = spark_session.read.parquet(path)
        assert_dataframe_equality(third_run_output_df, third_run_target_df)

        # tear down
        shutil.rmtree("test_folder")
