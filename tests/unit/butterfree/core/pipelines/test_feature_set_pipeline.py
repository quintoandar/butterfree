from unittest.mock import Mock

import pytest

from butterfree.core.clients import SparkClient
from butterfree.core.extract import Source
from butterfree.core.extract.readers import FileReader, TableReader
from butterfree.core.extract.readers.reader import Reader
from butterfree.core.load import Sink
from butterfree.core.load.writers import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
)
from butterfree.core.load.writers.writer import Writer
from butterfree.core.pipelines.feature_set_pipeline import FeatureSetPipeline
from butterfree.core.transform import FeatureSet
from butterfree.core.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.core.transform.transformations import AggregatedTransform


class TestFeatureSetPipeline:
    def test_feature_set_args(self):
        # arrange and act
        out_columns = [
            "user_id",
            "timestamp",
            "listing_page_viewed__rent_per_month__avg_over_7_days_fixed_windows",
            "listing_page_viewed__rent_per_month__avg_over_2_weeks_fixed_windows",
            "listing_page_viewed__rent_per_month__stddev_pop_over_7_days_fixed_windows",
            "listing_page_viewed__rent_per_month__stddev_pop_over_2_weeks_fixed_windows",  # noqa
        ]
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
                        name="user_id", description="The user's Main ID or device ID",
                    )
                ],
                timestamp=TimestampFeature(from_column="ts"),
                features=[
                    Feature(
                        name="listing_page_viewed__rent_per_month",
                        description="Average of something.",
                        transformation=AggregatedTransform(
                            aggregations=["avg", "stddev_pop"],
                            partition="user_id",
                            windows=["7 days", "2 weeks"],
                            mode=["fixed_windows"],
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

        assert isinstance(pipeline.spark_client, SparkClient)
        assert len(pipeline.source.readers) == 2
        assert all(isinstance(reader, Reader) for reader in pipeline.source.readers)
        assert isinstance(pipeline.source.query, str)
        assert pipeline.feature_set.name == "feature_set"
        assert pipeline.feature_set.entity == "entity"
        assert pipeline.feature_set.description == "description"
        assert isinstance(pipeline.feature_set.timestamp, TimestampFeature)
        assert len(pipeline.feature_set.keys) == 1
        assert all(isinstance(k, KeyFeature) for k in pipeline.feature_set.keys)
        assert len(pipeline.feature_set.features) == 1
        assert all(
            isinstance(feature, Feature) for feature in pipeline.feature_set.features
        )
        assert pipeline.feature_set.columns == out_columns
        assert len(pipeline.sink.writers) == 2
        assert all(isinstance(writer, Writer) for writer in pipeline.sink.writers)

    def test_run(self):
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
                        name="user_id", description="The user's Main ID or device ID",
                    )
                ],
                timestamp=TimestampFeature(from_column="ts"),
                features=[
                    Feature(
                        name="listing_page_viewed__rent_per_month",
                        description="Average of something.",
                        transformation=AggregatedTransform(
                            aggregations=["avg", "stddev_pop"],
                            partition="user_id",
                            windows=["7 days", "2 weeks"],
                            mode=["fixed_windows"],
                        ),
                    ),
                ],
            ),
            sink=Mock(
                spec=Sink, writers=[HistoricalFeatureStoreWriter(db_config=None)],
            ),
        )
        test_pipeline.run()

        test_pipeline.source.construct.assert_called_once()
        test_pipeline.feature_set.construct.assert_called_once()
        test_pipeline.sink.flush.assert_called_once()
        test_pipeline.sink.validate.assert_called_once()

    def test_source_raise(self):
        with pytest.raises(ValueError, match="source must be a Source instance"):
            FeatureSetPipeline(
                spark_client=SparkClient(),
                source=Mock(
                    spark_client=SparkClient(),
                    readers=[
                        TableReader(id="source_a", database="db", table="table",),
                    ],
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
                        )
                    ],
                    timestamp=TimestampFeature(from_column="ts"),
                    features=[
                        Feature(
                            name="listing_page_viewed__rent_per_month",
                            description="Average of something.",
                            transformation=AggregatedTransform(
                                aggregations=["avg", "stddev_pop"],
                                partition="user_id",
                                windows=["7 days", "2 weeks"],
                                mode=["fixed_windows"],
                            ),
                        ),
                    ],
                ),
                sink=Mock(
                    spec=Sink, writers=[HistoricalFeatureStoreWriter(db_config=None)],
                ),
            )

    def test_feature_set_raise(self):
        with pytest.raises(
            ValueError, match="feature_set must be a FeatureSet instance"
        ):
            FeatureSetPipeline(
                spark_client=SparkClient(),
                source=Mock(
                    spec=Source,
                    readers=[
                        TableReader(id="source_a", database="db", table="table",),
                    ],
                    query="select * from source_a",
                ),
                feature_set=Mock(
                    name="feature_set",
                    entity="entity",
                    description="description",
                    keys=[
                        KeyFeature(
                            name="user_id",
                            description="The user's Main ID or device ID",
                        )
                    ],
                    timestamp=TimestampFeature(from_column="ts"),
                    features=[
                        Feature(
                            name="listing_page_viewed__rent_per_month",
                            description="Average of something.",
                            transformation=AggregatedTransform(
                                aggregations=["avg", "stddev_pop"],
                                partition="user_id",
                                windows=["7 days", "2 weeks"],
                                mode=["fixed_windows"],
                            ),
                        ),
                    ],
                ),
                sink=Mock(
                    spec=Sink, writers=[HistoricalFeatureStoreWriter(db_config=None)],
                ),
            )

    def test_sink_raise(self):
        with pytest.raises(ValueError, match="sink must be a Sink instance"):
            FeatureSetPipeline(
                spark_client=SparkClient(),
                source=Mock(
                    spec=Source,
                    readers=[
                        TableReader(id="source_a", database="db", table="table",),
                    ],
                    query="select * from source_a",
                ),
                feature_set=Mock(
                    spec=FeatureSet,
                    name="feature_set",
                    entity="entity",
                    description="description",
                    features=[
                        Feature(
                            name="user_id",
                            description="The user's Main ID or device ID",
                        ),
                        Feature(name="ts", description="The timestamp feature",),
                    ],
                    key_columns=["user_id"],
                    timestamp_column="ts",
                ),
                sink=Mock(writers=[HistoricalFeatureStoreWriter(db_config=None)],),
            )
