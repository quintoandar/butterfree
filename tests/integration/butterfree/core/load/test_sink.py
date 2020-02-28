from tests.integration.butterfree.core.load.conftest import create_bucket

from butterfree.core.clients import CassandraClient, SparkClient
from butterfree.core.configs import environment
from butterfree.core.configs.db import CassandraConfig, S3Config
from butterfree.core.load import Sink
from butterfree.core.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)


def test_sink(input_dataframe, feature_set):
    # arrange
    spark_client = SparkClient()
    cassandra_client = CassandraClient()
    feature_set_df = feature_set.construct(input_dataframe)
    target_latest_df = OnlineFeatureStoreWriter.filter_latest(
        feature_set_df, id_columns=[key.name for key in feature_set.keys]
    )
    columns_sort = feature_set_df.schema.fieldNames()

    # setup historical writer
    create_bucket(environment.get_variable("FEATURE_STORE_S3_BUCKET"))
    s3_config = S3Config()
    historical_writer = HistoricalFeatureStoreWriter(db_config=s3_config)

    # setup online writer
    online_config = CassandraConfig()
    online_writer = OnlineFeatureStoreWriter(db_config=online_config)

    writers = [online_writer, historical_writer]
    sink = Sink(writers)

    # # act
    spark_client.sql(
        "CREATE DATABASE IF NOT EXISTS {}".format(historical_writer.database)
    )
    cassandra_client.sql(
        "CREATE KEYSPACE IF NOT EXISTS "
        f"{environment.get_variable('CASSANDRA_KEYSPACE')} "
        "WITH REPLICATION = {'class':'SimpleStrategy', "
        "'replication_factor':1};"
    )
    cassandra_client.sql(
        "CREATE TABLE IF NOT EXISTS "
        f"{environment.get_variable('CASSANDRA_KEYSPACE')}.test_sink_feature_set "
        "(id int, timestamp timestamp, "
        "feature int, PRIMARY KEY (id));"
    )
    sink.flush(feature_set, feature_set_df, spark_client)

    # get historical results
    historical_result_df = spark_client.read_table(
        historical_writer.database, feature_set.name
    )

    # get online results
    online_result_df = spark_client.read(
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
