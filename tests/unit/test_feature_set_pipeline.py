from pyspark.sql import DataFrame

from butterfree.core.feature_set_pipeline import FeatureSetPipeline
from butterfree.core.loader.online_feature_store_loader import OnlineFeatureStoreLoader
from butterfree.core.source.table_source import TableSource
from butterfree.core.transform.aggregation import AggregatedTransform
from butterfree.core.transform.feature import Feature


def create_temp_view(dataframe: DataFrame, name):
    dataframe.createOrReplaceTempView(name)


class TestFeatureSetPipeline:
    def test_feature_set_args(self):

        pipeline = FeatureSetPipeline(
            name="feature set",
            entity="entity",
            description="This is a feature set.",
            readers=[TableSource],
            query="select * from source",
            features=[
                Feature(name="user_id", description="The user's Main ID or device ID",),
                Feature(
                    name="listing_page_viewed__rent_per_month",
                    description="Average of the rent per month of listings viewed",
                ).add(
                    AggregatedTransform(
                        aggregations=["avg", "std"],
                        partition="user_id",
                        windows={"days": [7], "weeks": [2]},
                    )
                ),
            ],
            writers=[OnlineFeatureStoreLoader],
        )

        assert pipeline.name == "feature set"
        assert pipeline.entity == "entity"
        assert pipeline.description == "This is a feature set."
        assert pipeline.readers == [TableSource]
        assert pipeline.query == "select * from source"
        assert len(pipeline.features) == 2
        assert pipeline.writers == [OnlineFeatureStoreLoader]
