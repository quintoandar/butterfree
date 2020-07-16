import shutil
from unittest.mock import Mock

from butterfree.clients import SparkClient
from butterfree.load import Sink
from butterfree.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)


def test_sink(input_dataframe, feature_set):
    # arrange
    client = SparkClient()
    feature_set_df = feature_set.construct(input_dataframe, client)
    target_latest_df = OnlineFeatureStoreWriter.filter_latest(
        feature_set_df, id_columns=[key.name for key in feature_set.keys]
    )
    columns_sort = feature_set_df.schema.fieldNames()

    # setup historical writer
    s3config = Mock()
    s3config.get_options = Mock(
        return_value={
            "mode": "overwrite",
            "format_": "parquet",
            "path": "test_folder/historical/entity/feature_set",
        }
    )
    historical_writer = HistoricalFeatureStoreWriter(db_config=s3config)

    # setup online writer
    # TODO: Change for CassandraConfig when Cassandra for test is ready
    online_config = Mock()
    online_config.mode = "overwrite"
    online_config.format_ = "parquet"
    online_config.get_options = Mock(
        return_value={"path": "test_folder/online/entity/feature_set"}
    )
    online_writer = OnlineFeatureStoreWriter(db_config=online_config)

    writers = [historical_writer, online_writer]
    sink = Sink(writers)

    # act
    client.sql("CREATE DATABASE IF NOT EXISTS {}".format(historical_writer.database))
    sink.flush(feature_set, feature_set_df, client)

    # get historical results
    historical_result_df = client.read_table(
        feature_set.name, historical_writer.database
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

    # tear down
    shutil.rmtree("test_folder")
