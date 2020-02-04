from tests.integration import OUTPUT_PATH

from butterfree.core.client import SparkClient
from butterfree.core.db.configs import S3Config
from butterfree.core.writer import HistoricalFeatureStoreWriter, Sink


def test_writer(input_dataframe, feature_set):
    # arrange
    client = SparkClient()
    feature_set_df = feature_set.construct(input_dataframe)

    # setup historical writer
    s3_config = S3Config(path=OUTPUT_PATH + "/feature_store")
    historical_writer = HistoricalFeatureStoreWriter(db_config=s3_config)

    # TODO: setup online writer

    writers = [historical_writer]
    sink = Sink(writers)

    # act
    client.sql("CREATE DATABASE IF NOT EXISTS {}".format(s3_config.database))
    sink.flush(feature_set, feature_set_df, client)

    # get historical results
    historical_result_df = client.read_table(s3_config.database, feature_set.name)

    # TODO: get online results

    # assert historical results
    columns_sort = feature_set_df.schema.fieldNames()
    assert sorted(feature_set_df.select(*columns_sort).collect()) == sorted(
        historical_result_df.select(*columns_sort).collect()
    )

    # TODO: assert online results
