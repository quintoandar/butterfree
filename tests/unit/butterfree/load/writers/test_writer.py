from butterfree.clients import SparkClient
from butterfree.load.writers import HistoricalFeatureStoreWriter


class TestWriter:
    def test_build(
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
        historical_writer = HistoricalFeatureStoreWriter()
        feature_set = mocker.stub("feature_set")
        feature_set.entity = "house"
        feature_set.name = "test"

        # act
        historical_writer.build(feature_set, feature_set_dataframe, spark_client)

        # assert
        historical_writer.build.assert_called_once()
