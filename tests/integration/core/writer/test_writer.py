from unittest.mock import Mock

from tests.integration import OUTPUT_PATH

from butterfree.core.client import SparkClient
from butterfree.core.db.configs import S3Config
from butterfree.core.writer import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
    Sink,
)


def test_writer(input_dataframe, feature_set):
    # arrange
    client = SparkClient()
    feature_set_df = feature_set.construct(input_dataframe)
    target_latest_df = OnlineFeatureStoreWriter.filter_latest(
        feature_set_df, id_columns=[key.name for key in feature_set.keys]
    )
    columns_sort = feature_set_df.schema.fieldNames()

    # setup historical writer
    historical_config = S3Config(path=OUTPUT_PATH + "/historical/feature_store")
    historical_writer = HistoricalFeatureStoreWriter(db_config=historical_config)

    # setup online writer
    # TODO: Change for CassandraConfig when Cassandra for test is ready
    online_config = S3Config(path=OUTPUT_PATH + "/online/feature_store")
    online_config.get_options = Mock(
        return_value={
            "path": "{}/{}/{}/".format(
                online_config.path, feature_set.entity, feature_set.name
            )
        }
    )
    online_writer = OnlineFeatureStoreWriter(db_config=online_config)

    writers = [historical_writer, online_writer]
    sink = Sink(writers)

    # act
    client.sql("CREATE DATABASE IF NOT EXISTS {}".format(historical_config.database))
    sink.flush(feature_set, feature_set_df, client)

    # get historical results
    historical_result_df = client.read_table(
        historical_config.database, feature_set.name
    )

    # get online results
    online_result_df = client.read(
        online_config.format_, options=online_config.get_options(feature_set.name)
    )

    # assert
    # assert historical results
    assert sorted(feature_set_df.select(*columns_sort).collect()) == sorted(
        historical_result_df.select(*columns_sort).collect()
    )

    # assert online results
    assert sorted(target_latest_df.select(*columns_sort).collect()) == sorted(
        online_result_df.select(*columns_sort).collect()
    )
