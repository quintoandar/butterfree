from unittest.mock import Mock

import pytest

from butterfree.core.client import SparkClient
from butterfree.core.feature_set_pipeline import FeatureSetPipeline
from butterfree.core.reader import FileReader, Reader, Source, TableReader
from butterfree.core.transform import Feature, FeatureSet
from butterfree.core.transform.aggregation import AggregatedTransform
from butterfree.core.writer import (
    HistoricalFeatureStoreWriter,
    OnlineFeatureStoreWriter,
    Sink,
    Writer,
)


class TestFeatureSetPipeline:
    def test_feature_set_args(self):
        pipeline = FeatureSetPipeline(
            spark_client=SparkClient(),
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
                features=[
                    Feature(
                        name="user_id", description="The user's Main ID or device ID",
                    ),
                    Feature(
                        name="listing_page_viewed__rent_per_month",
                        description="Average of the rent per month of listings viewed",
                        transformation=AggregatedTransform(
                            aggregations=["avg", "std"],
                            partition="user_id",
                            windows=["7 days", "2 weeks"],
                        ),
                    ),
                    Feature(name="ts", description="The timestamp feature",),
                ],
                key_columns=["user_id"],
                timestamp_column="ts",
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
        assert pipeline.feature_set.timestamp_column == "ts"
        assert len(pipeline.feature_set.key_columns) == 1
        assert all(
            isinstance(key_column, str)
            for key_column in pipeline.feature_set.key_columns
        )
        assert len(pipeline.feature_set.features) == 3
        assert all(
            isinstance(feature, Feature) for feature in pipeline.feature_set.features
        )
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
                features=[
                    Feature(
                        name="user_id", description="The user's Main ID or device ID",
                    ),
                    Feature(name="ts", description="The timestamp feature",),
                ],
                key_columns=["user_id"],
                timestamp_column="ts",
            ),
            sink=Mock(
                spec=Sink, writers=[HistoricalFeatureStoreWriter(db_config=None)],
            ),
        )
        test_pipeline.run()

        assert test_pipeline.source.construct.assert_called_once
        assert test_pipeline.feature_set.construct.assert_called_once
        assert test_pipeline.sink.flush.assert_called_once
        assert test_pipeline.sink.validate.assert_called_once

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
