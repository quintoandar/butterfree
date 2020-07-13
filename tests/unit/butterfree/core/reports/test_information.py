from pyspark.sql import functions

from butterfree import (
    DataType,
    Feature,
    FeatureSet,
    FeatureSetPipeline,
    FileReader,
    Function,
    HistoricalFeatureStoreWriter,
    KeyFeature,
    OnlineFeatureStoreWriter,
    Sink,
    Source,
    SparkFunctionTransform,
    TableReader,
    TimestampFeature,
)
from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.reports.information import Metadata


class TestMetadata:
    def test_json(self):
        pipeline = FeatureSetPipeline(
            source=Source(
                readers=[
                    TableReader(id="source_a", database="db", table="table",),
                    FileReader(id="source_b", path="path", format="parquet",),
                ],
                query="select a.*, b.specific_feature "
                "from source_a left join source_b on a.id=b.id",
            ),
            feature_set=FeatureSet(
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
                                Function(functions.stddev_pop, DataType.DOUBLE),
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
            sink=Sink(
                writers=[
                    HistoricalFeatureStoreWriter(db_config=None),
                    OnlineFeatureStoreWriter(db_config=None),
                ],
            ),
        )

        info = Metadata(pipeline, save=True)
        info.to_json()
        info.to_markdown()
        print(info)
