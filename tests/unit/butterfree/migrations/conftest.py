from pytest import fixture

from butterfree.clients import SparkClient
from butterfree.constants import DataType
from butterfree.extract import Source
from butterfree.extract.readers import TableReader
from butterfree.load import Sink
from butterfree.load.writers import HistoricalFeatureStoreWriter
from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature


@fixture()
def feature_set_pipeline():
    test_pipeline = FeatureSetPipeline(
        spark_client=SparkClient(),
        source=Source(
            readers=[TableReader(id="source_a", database="db", table="table",)],
            query="select * from source_a",
        ),
        feature_set=FeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            keys=[
                KeyFeature(
                    name="id",
                    description="The user's Main ID or device ID",
                    dtype=DataType.INTEGER,
                )
            ],
            timestamp=TimestampFeature(from_column="ts"),
            features=[
                Feature(
                    name="feature_avg",
                    description="Average of something.",
                    dtype=DataType.FLOAT,
                ),
            ],
        ),
        sink=Sink(writers=[HistoricalFeatureStoreWriter(db_config=None)],),
    )

    return test_pipeline
