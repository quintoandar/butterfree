from unittest import mock

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from butterfree.clients import SparkClient
from butterfree.configs.db.delta import DeltaConfig
from butterfree.constants import DataType
from butterfree.extract import Source
from butterfree.extract.readers import TableReader
from butterfree.load import Sink
from butterfree.load.writers import DeltaFeatureStoreWriter
from butterfree.load.writers.delta_writer import DeltaWriter
from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature


@pytest.fixture
def sample_dataframe(spark_client):
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("feature", StringType(), True),
            StructField("timestamp", StringType(), False),
        ]
    )
    data = [(1, "value1", "2024-01-01"), (2, "value2", "2024-01-02")]
    return spark_client.conn.createDataFrame(data, schema=schema)


@pytest.fixture
def spark_client(spark_session):
    client = mock.MagicMock(spec=SparkClient)
    client.conn = spark_session
    return client


class TestDeltaConfig:
    def test_initialization(self):
        config = DeltaConfig(
            database="test_db",
            table="test_table",
            merge_on=["id"],
        )
        assert config.database == "test_db"
        assert config.table == "test_table"
        assert config.merge_on == ["id"]
        assert config.format_ == "delta"
        assert config.mode == "overwrite"

    def test_get_options(self):
        config = DeltaConfig(
            database="test_db",
            table="test_table",
            merge_on=["id"],
        )
        options = config.get_options("test_table")
        assert options["database"] == "test_db"
        assert options["table"] == "test_table"


class TestDeltaWriter:

    def test_merge(self, spark_client, sample_dataframe):
        """Test merge function with a mocked DeltaTable."""
        with (
            mock.patch(
                "butterfree.load.writers.delta_writer.DeltaTable.forName"
            ) as mock_delta_table,
            mock.patch.object(DeltaWriter, "_convert_to_delta") as mock_convert,  # noqa
        ):
            # Mock table existence and describe
            spark_client.conn.catalog.tableExists = mock.MagicMock(return_value=True)
            mock_table = mock.MagicMock()
            mock_delta_table.return_value = mock_table

            # Run merge
            DeltaWriter().merge(
                client=spark_client,
                database="test_db",
                table="test_table",
                merge_on=["id"],
                source_df=sample_dataframe,
            )

            # Ensure DeltaTable.merge() was called
            mock_table.alias.assert_called_once_with("target")
            mock_table.alias.return_value.merge.assert_called_once()

    def test_vacuum(self, spark_client):
        """Test vacuum operation."""
        with mock.patch.object(spark_client.conn, "sql") as mock_sql:
            DeltaWriter().vacuum("test_table", 24, spark_client)
            mock_sql.assert_called_once_with("VACUUM test_table RETAIN 24 HOURS")

    def test_optimize(self, spark_client):
        """Test optimize operation."""
        with mock.patch.object(spark_client.conn, "sql") as mock_sql:
            DeltaWriter().optimize(spark_client, table="test_table")
            mock_sql.assert_called_once_with("OPTIMIZE test_table")

    def test_convert_to_delta_already_delta(self, spark_client):
        """Ensure no conversion happens if table is already Delta."""
        schema = StructType([StructField("format", StringType(), False)])
        mock_df = spark_client.conn.createDataFrame([("delta",)], schema=schema)

        with mock.patch.object(
            spark_client.conn, "sql", return_value=mock_df
        ) as mock_sql:
            DeltaWriter()._convert_to_delta(spark_client, "test_table")

            mock_sql.assert_any_call("DESCRIBE DETAIL test_table")
            calls = [call[0][0].strip() for call in mock_sql.call_args_list]
            assert "CONVERT TO DELTA test_table" not in calls  # Ensure no conversion

    def test_convert_to_delta_not_delta(self, spark_client):
        """Ensure conversion happens if table is NOT Delta."""
        schema = StructType([StructField("format", StringType(), False)])
        mock_df = spark_client.conn.createDataFrame([("parquet",)], schema=schema)

        with mock.patch.object(
            spark_client.conn, "sql", side_effect=[mock_df, None, None]
        ) as mock_sql:
            DeltaWriter()._convert_to_delta(spark_client, "test_table")

            expected_calls = [
                "DESCRIBE DETAIL test_table",
                "CONVERT TO DELTA test_table",
                """ALTER TABLE test_table
                    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""",
            ]

            actual_calls = [call[0][0].strip() for call in mock_sql.call_args_list]
            for expected in expected_calls:
                assert (
                    expected in actual_calls
                ), f"Missing expected SQL call: {expected}"


class TestDeltaFeatureStoreWriter:
    def test_write(self, spark_client, sample_dataframe):
        writer = DeltaFeatureStoreWriter(
            database="test_db",
            table="test_table",
            merge_on=["id"],
        )

        with mock.patch(
            "butterfree.load.writers.delta_writer.DeltaWriter.merge"
        ) as mock_merge:
            writer.write(sample_dataframe, spark_client, mock.Mock())
            mock_merge.assert_called_once_with(
                client=spark_client,
                database="test_db",
                table="test_table",
                merge_on=["id"],
                source_df=sample_dataframe,
                when_not_matched_insert=None,
                when_matched_update=None,
                when_matched_delete=None,
            )

    def test_validate(self, spark_client, sample_dataframe):
        writer = DeltaFeatureStoreWriter(
            database="test_db",
            table="test_table",
            merge_on=["id"],
        )
        assert writer.validate(sample_dataframe, spark_client, mock.Mock()) is None

    def test_check_schema(self, spark_client, sample_dataframe):
        writer = DeltaFeatureStoreWriter(
            database="test_db",
            table="test_table",
            merge_on=["id"],
        )
        assert writer.check_schema(sample_dataframe, []) is None


class TestFeatureSetPipeline:
    def test_feature_set_pipeline_with_delta_writer(
        self, spark_client, sample_dataframe
    ):
        pipeline = FeatureSetPipeline(
            source=Source(
                readers=[TableReader(id="id", database="db", table="table")],
                query="SELECT * FROM table",
            ),
            feature_set=FeatureSet(
                name="feature_set",
                entity="entity",
                description="description",
                features=[
                    Feature(name="feature", description="test", dtype=DataType.FLOAT)
                ],
                keys=[KeyFeature(name="id", description="id", dtype=DataType.INTEGER)],
                timestamp=TimestampFeature(),
                deduplicate_rows=True,
            ),
            sink=Sink(
                writers=[
                    DeltaFeatureStoreWriter(
                        database="test_db", table="test_table", merge_on=["id"]
                    )
                ]
            ),
            spark_client=spark_client,
        )

        pipeline.source.construct = mock.Mock(return_value=sample_dataframe)
        pipeline.feature_set.construct = mock.Mock(return_value=sample_dataframe)

        with mock.patch(
            "butterfree.load.writers.delta_writer.DeltaWriter.merge"
        ) as mock_merge:
            pipeline.run()
            pipeline.source.construct.assert_called_once()
            pipeline.feature_set.construct.assert_called_once()
            mock_merge.assert_called_once()
