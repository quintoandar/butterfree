from butterfree.constants.data_type import DataType
from butterfree.extract import Source
from butterfree.extract.readers import TableReader
from butterfree.load import Sink
from butterfree.load.writers import HistoricalFeatureStoreWriter, OnlineFeatureStoreWriter
from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature


class SecondPipeline(FeatureSetPipeline):
    def __init__(self):
        super(SecondPipeline, self).__init__(
            source=Source(
                readers=[
                    TableReader(
                        id="t",
                        database="db",
                        table="table",
                    )
                ],
                query=f"select * from t",  # noqa
            ),
            feature_set=FeatureSet(
                name="second",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature1",
                        description="test",
                        dtype=DataType.STRING,
                    ),
                    Feature(
                        name="feature2",
                        description="another test",
                        dtype=DataType.FLOAT,
                    ),
                ],
                keys=[
                    KeyFeature(
                        name="id",
                        description="identifier",
                        dtype=DataType.BIGINT,
                    )
                ],
                timestamp=TimestampFeature(),
            ),
            sink=Sink(
                writers=[HistoricalFeatureStoreWriter(), OnlineFeatureStoreWriter()]
            ),
        )
