from butterfree.core.feature_set_pipeline import FeatureSetPipeline
from butterfree.core.reader import FileReader, KafkaReader, TableReader
from butterfree.core.transform.aggregation import AggregatedTransform
from butterfree.core.transform.feature import Feature
from butterfree.core.writer import OnlineFeatureStoreWriter, HistoricalFeatureStoreWriter


class TestFeatureSetPipeline:
    def test_feature_set_args(self):

        pipeline = FeatureSetPipeline(
            name="feature set",
            entity="entity",
            description="This is a feature set.",
            key_columns=["id"],
            timestamp_column="ts",
            readers=[TableReader, FileReader, KafkaReader],
            query="select * from source",
            features=[
                Feature(name="user_id", description="The user's Main ID or device ID",),
                Feature(
                    name="listing_page_viewed__rent_per_month",
                    description="Average of the rent per month of listings viewed",
                    transformation=AggregatedTransform(
                        aggregations=["avg", "std"],
                        partition="user_id",
                        windows=["7 days", "2 weeks"],
                    ),
                ),
            ],
            writers=[OnlineFeatureStoreWriter, HistoricalFeatureStoreWriter],
        )

        assert pipeline.name == "feature set"
        assert pipeline.entity == "entity"
        assert pipeline.description == "This is a feature set."
        assert len(pipeline.key_columns) == 1
        assert pipeline.timestamp_column == "ts"
        assert len(pipeline.readers) == 3
        assert pipeline.query == "select * from source"
        assert len(pipeline.features) == 2
        assert len(pipeline.writers) == 2
