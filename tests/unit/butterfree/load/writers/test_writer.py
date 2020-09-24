from butterfree.clients import SparkClient
from butterfree.configs.db import S3Config
from butterfree.constants import columns
from butterfree.load.writers import HistoricalFeatureStoreWriter


class TestWriter:
    def test_write(
        self,
        feature_set_dataframe,
        historical_feature_set_dataframe,
        mocker,
        feature_set,
    ):
        # arrange
        spark_client = SparkClient()
        spark_client.conn.conf.set(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )
        spark_client.write_dataframe = mocker.stub("write_dataframe")

        historical_writer = HistoricalFeatureStoreWriter()
        db_config = S3Config()
        options = {"path": "test_folder/historical/entity/feature_set"}
        partition_by = [
            columns.PARTITION_YEAR,
            columns.PARTITION_MONTH,
            columns.PARTITION_DAY,
        ]

        # act
        historical_writer.write(
            spark_client=spark_client,
            load_df=feature_set_dataframe,
            table_name=feature_set.name,
            db_config=db_config,
            options=options,
            partition_by=partition_by,
        )

        # assert
        spark_client.write_dataframe.assert_called_once()
