from unittest.mock import Mock

from pyspark.sql import functions
from pytest import fixture

from butterfree.clients import SparkClient
from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.extract import Source
from butterfree.extract.readers import TableReader
from butterfree.load import Sink
from butterfree.load.writers import HistoricalFeatureStoreWriter
from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import SparkFunctionTransform
from butterfree.transform.utils import Function


@fixture()
def feature_set_pipeline():
    test_pipeline = FeatureSetPipeline(
        spark_client=SparkClient(),
        source=Mock(
            spec=Source,
            readers=[TableReader(id="source_a", database="db", table="table",)],
            query="select * from source_a",
        ),
        feature_set=Mock(
            spec=FeatureSet,
            name="feature_set",
            entity="entity",
            description="description",
            keys=[
                KeyFeature(
                    name="user_id",
                    description="The user's Main ID or device ID",
                    dtype=DataType.INTEGER,
                )
            ],
            timestamp=TimestampFeature(from_column="ts"),
            features=[
                Feature(
                    name="listing_page_viewed__rent_per_month",
                    description="Average of something.",
                    transformation=SparkFunctionTransform(
                        functions=[
                            Function(functions.avg, DataType.FLOAT),
                            Function(functions.stddev_pop, DataType.FLOAT),
                        ],
                    ).with_window(
                        partition_by="user_id",
                        order_by=TIMESTAMP_COLUMN,
                        window_definition=["7 days", "2 weeks"],
                        mode="fixed_windows",
                    ),
                ),
            ],
        ),
        sink=Mock(spec=Sink, writers=[HistoricalFeatureStoreWriter(db_config=None)],),
    )

    return test_pipeline
