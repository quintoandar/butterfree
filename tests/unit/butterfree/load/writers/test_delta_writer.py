import os

import pytest

from butterfree.clients import SparkClient
from butterfree.load.writers import DeltaWriter, HistoricalFeatureStoreWriter

DELTA_LOCATION = "spark-warehouse"

class TestDeltaWriter:

    def __checkFileExists(self, file_name: str = "test_delta_table") -> bool:
        return os.path.exists(os.path.join(DELTA_LOCATION, file_name))

    def test_merge(self):

        client = SparkClient()

        # create_delta_table(client)
        client.conn.sql(
            "CREATE TABLE test_delta_table (id INT, feature STRING) USING DELTA "
        )
        client.conn.sql(
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

        assert df is not None
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

        assert df is not None
        assert df.toPandas().feature[0] == "test2"

        client.conn.sql("DROP TABLE test_delta_table")

    def test_merge_from_historical_writer(
        self, feature_set, feature_set_dataframe
    ):
        # given
        client = SparkClient()
        writer = HistoricalFeatureStoreWriter()
        client.conn.sql("CREATE SCHEMA test")
        client.conn.sql(
            """CREATE TABLE test.feature_set
            (id INT, feature STRING, timestamp TIMESTAMP) USING DELTA """
        )
        client.conn.sql(
            """INSERT INTO test.feature_set(id, feature, timestamp)
            VALUES(1, 'test', cast(date_format('2019-12-31', 'yyyy-MM-dd') as timestamp))"""
        )

        # when
        writer.write(
            feature_set=feature_set,
            dataframe=feature_set_dataframe,
            spark_client=client,
            merge_on=["id", "timestamp"],
        )

        result_df = client.conn.read.table("test.feature_set")
        rpd = result_df.toPandas()
        rpd_filtered = rpd.loc[(rpd['id']==1) & (rpd['timestamp'] == '2019-12-31')]

        assert result_df is not None
        assert str(rpd_filtered.feature.values[0]) == '100'

        client.conn.sql("DROP TABLE test.feature_set")
        client.conn.sql("DROP SCHEMA test")

    def test_optimize(self):

        client = SparkClient()
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

    def test_vacuum(self):

        client = SparkClient()

        client.conn.sql(
            "CREATE TABLE test_delta_table_v (id INT, feature STRING) USING DELTA "
        )
        client.conn.sql(
            "INSERT INTO test_delta_table_v(id, feature) VALUES(1, 'test') "
        )

        client.conn.conf.set(
            "spark.databricks.delta.retentionDurationCheck.enabled", "false"
        )

        retention_hours = 24

        DeltaWriter().vacuum("test_delta_table_v", retention_hours, client)

        assert self.__checkFileExists("test_delta_table_v") is True

        client.conn.sql("DROP TABLE test_delta_table_v")
