import pytest
from unittest.mock import Mock

from butterfree.core.writer import (
    HistoricalFeatureStoreWriter,
    Sink,
)
from butterfree.core.client import SparkClient
from butterfree.core.transform import Feature, FeatureSet
from butterfree.core.db.configs import S3Config


def test_sink(feature_set_dataframe):
    # arrange
    features = [
        Feature(name="id", description="Description"),
        Feature(name="ts", description="Description"),
        Feature(name="feature", description="Description"),
        # TODO: remove partition features when implemented in historical writer
        Feature(name="partition__year", description="Description"),
        Feature(name="partition__month", description="Description"),
        Feature(name="partition__day", description="Description"),
    ]
    feature_set = FeatureSet(
        "test_sink_feature_set",
        "test_sink_entity",
        "description",
        features,
        key_columns=["id"],
        timestamp_column="ts",
    )
    client = SparkClient()

    # setup historical writer
    s3_config = S3Config(path="/tmp/feature_store/")
    historical_writer = HistoricalFeatureStoreWriter(client, db_config=s3_config)

    # TODO: setup online writer

    writers = [historical_writer]
    sink = Sink(writers)

    # act
    client.sql("CREATE DATABASE IF NOT EXISTS {}".format(s3_config.database))
    sink.flush(feature_set, feature_set_dataframe)

    # get historical results
    historical_result_df = client.sql(
        "select * from {}.{}".format(s3_config.database, feature_set.name)
    )

    # TODO: get online results

    # assert historical results
    columns_sort = feature_set_dataframe.schema.fieldNames()
    assert sorted(feature_set_dataframe.select(*columns_sort).collect()) == sorted(
        historical_result_df.select(*columns_sort).collect()
    )

    # TODO: assert online results
