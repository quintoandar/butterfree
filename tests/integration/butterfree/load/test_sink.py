import shutil
from unittest.mock import Mock

from butterfree.clients import SparkClient
from butterfree.load import Sink
from butterfree.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)


def test_sink(input_dataframe, feature_set, mocker):
    # arrange
    client = SparkClient()
    client.conn.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    feature_set_df = feature_set.construct(input_dataframe, client)
    target_latest_df = OnlineFeatureStoreWriter.filter_latest(
        feature_set_df, id_columns=[key.name for key in feature_set.keys]
    )
    columns_sort = feature_set_df.schema.fieldNames()

    # setup historical writer
    s3config = Mock()
    s3config.mode = "overwrite"
    s3config.format_ = "parquet"
    s3config.get_options = Mock(
        return_value={"path": "test_folder/historical/entity/feature_set"}
    )
    s3config.get_path_with_partitions = Mock(
        return_value="test_folder/historical/entity/feature_set"
    )

    historical_writer = HistoricalFeatureStoreWriter(
        db_config=s3config, interval_mode=True
    )

    # setup online writer
    # TODO: Change for CassandraConfig when Cassandra for test is ready
    online_config = Mock()
    online_config.mode = "overwrite"
    online_config.format_ = "parquet"
    online_config.get_options = Mock(
        return_value={"path": "test_folder/online/entity/feature_set"}
    )
    online_writer = OnlineFeatureStoreWriter(db_config=online_config)

    online_writer.check_schema_hook = mocker.stub("check_schema_hook")
    online_writer.check_schema_hook.run = mocker.stub("run")
    online_writer.check_schema_hook.run.return_value = feature_set_df

    writers = [historical_writer, online_writer]
    sink = Sink(writers)

    # act
    client.sql("CREATE DATABASE IF NOT EXISTS {}".format(historical_writer.database))
    sink.flush(feature_set, feature_set_df, client)

    # get historical results
    historical_result_df = client.read(
        s3config.format_,
        path=s3config.get_path_with_partitions(feature_set.name, feature_set_df),
    )

    # get online results
    online_result_df = client.read(
        online_config.format_, **online_config.get_options(feature_set.name)
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
