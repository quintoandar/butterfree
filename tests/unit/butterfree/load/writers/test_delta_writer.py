import os
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import pytest
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, DataFrameReader, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from pyspark.testing import assertDataFrameEqual

from butterfree.clients import SparkClient
from butterfree.configs.db import MetastoreConfig
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.load.writers import DeltaWriter, HistoricalFeatureStoreWriter

DELTA_LOCATION = "spark-warehouse"


@pytest.fixture
def client_fixture():
    os.environ["SPARK_MASTER_HOST"] = "127.0.0.1"
    client = SparkClient()
    yield client
    client.conn.stop()


# @pytest.fixture
# def create_delta_table(client_fixture):

#     client_fixture.conn.sql(
#         "CREATE TABLE test_delta_table (id INT, feature STRING) USING DELTA "
#     )
#     client_fixture.conn.sql(
#         "INSERT INTO test_delta_table(id, feature) VALUES(1, 'test') "
#     )
# yield "created"
# client.conn.sql("DROP TABLE test_delta_table")


class TestDeltaWriter:

    def __checkFileExists(self, file_name: str = "test_delta_table") -> bool:
        return os.path.exists(os.path.join(DELTA_LOCATION, file_name))

    def test_merge(self, client_fixture):

        client = client_fixture

        # create_delta_table(client)
        client_fixture.conn.sql(
            "CREATE TABLE test_delta_table (id INT, feature STRING) USING DELTA "
        )
        client_fixture.conn.sql(
            "INSERT INTO test_delta_table(id, feature) VALUES(1, 'test') "
        )

        source = client.conn.createDataFrame([(1, "test2")], ["id", "feature"])

        DeltaWriter().merge(
            client,
            None,
            "test_delta_table",
            "spark-warehouse/test_delta_table",
            ["id"],
            source,
        )

        df = client.conn.read.table("test_delta_table")

        assert df != None
        assert df.toPandas().feature[0] == "test2"

        # Step 2
        source = client.conn.createDataFrame(
            [(1, "test3"), (2, "test4"), (3, "test5")], ["id", "feature"]
        )

        DeltaWriter().merge(
            client,
            None,
            "test_delta_table",
            "spark-warehouse/test_delta_table",
            ["id"],
            source,
            None,
            "id > 2",
        )

        df = client.conn.read.table("test_delta_table")

        assert df != None
        assert df.toPandas().feature[0] == "test2"

        client.conn.sql("DROP TABLE test_delta_table")

    def test_merge_from_historical_writer(self, feature_set, feature_set_dataframe, client_fixture):
        # given
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        # spark_client = SparkClient()
        writer = HistoricalFeatureStoreWriter()
        client_fixture.conn.sql(
            "CREATE TABLE test_delta_table_from_hist (id INT, feature STRING, ts_feature TIMESTAMP) USING DELTA "
        )
        client_fixture.conn.sql(
            "INSERT INTO test_delta_table_from_hist(id, feature, ts_feature) VALUES(1, 'test', TO_DATE('2019-12-31', 'YYYY-MM-DD')) "
        )

        # when
        writer.write(
            feature_set=feature_set,
            dataframe=feature_set_dataframe,
            spark_client=client_fixture,
            merge_on=["id", "timestamp"],
        )

        result_df = client_fixture.conn.read.table("test_delta_table_from_hist")

        assert result_df != None
        assert result_df.toPandas().feature == 100

        client_fixture.conn.sql("DROP TABLE test_delta_table_from_hist")

    def test_optimize(self, client_fixture):

        client = client_fixture
        temp_file = "test_delta"

        df = client.conn.createDataFrame(
            [("a", 1), ("a", 2)], ["key", "value"]
        ).repartition(1)
        df.write.mode("overwrite").format("delta").save(temp_file)
        df = client.conn.createDataFrame(
            [("a", 3), ("a", 4)], ["key", "value"]
        ).repartition(1)
        df.write.format("delta").save(temp_file, mode="append")
        df = client.conn.createDataFrame(
            [("b", 1), ("b", 2)], ["key", "value"]
        ).repartition(1)
        df.write.format("delta").save(temp_file, mode="append")

        dw = DeltaWriter()

        # dw.optimize = MagicMock(spark_client_fixture)
        dw.optimize(client)

        # assertions
        # dw.optimize.assert_called_once_with(spark_client_fixture)

    def test_vacuum(self, client_fixture):

        client = client_fixture

        client_fixture.conn.sql(
            "CREATE TABLE test_delta_table_v (id INT, feature STRING) USING DELTA "
        )
        client_fixture.conn.sql(
            "INSERT INTO test_delta_table_v(id, feature) VALUES(1, 'test') "
        )

        client.conn.conf.set(
            "spark.databricks.delta.retentionDurationCheck.enabled", "false"
        )

        retention_hours = 24

        DeltaWriter().vacuum("test_delta_table_v", retention_hours, client)

        assert self.__checkFileExists("test_delta_table_v") == True

        client.conn.sql("DROP TABLE test_delta_table_v")
