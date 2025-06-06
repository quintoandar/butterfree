from pyspark.sql import functions as F

from butterfree.configs.db import MetastoreConfig
from butterfree.constants import DataType
from butterfree.extract import Source
from butterfree.extract.readers import TableReader
from butterfree.load import Sink
from butterfree.load.writers import HistoricalFeatureStoreWriter
from butterfree.metadata.feature_set_pipeline_metadata import FeatureSetPipelineMetadata
from butterfree.pipelines.feature_set_pipeline import FeatureSetPipeline
from butterfree.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import AggregatedTransform
from butterfree.transform.utils import Function


def test_build_metadata_with_aggregated_fs():
    # arrange
    pipeline = FeatureSetPipeline(
        source=Source(
            readers=[
                TableReader(
                    id="table_reader_id",
                    database="table_reader_db",
                    table="table_reader_table",
                ),
            ],
            query="select * from table_reader_id",
        ),
        feature_set=AggregatedFeatureSet(
            name="feature_set",
            entity="entity",
            description="description",
            features=[
                Feature(
                    name="feature1",
                    description="test",
                    transformation=AggregatedTransform(
                        functions=[Function(F.avg, DataType.DOUBLE)]
                    ),
                ),
            ],
            keys=[
                KeyFeature(
                    name="id",
                    description="The user's Main ID",
                    dtype=DataType.INTEGER,
                )
            ],
            timestamp=TimestampFeature(),
        ).with_windows(definitions=["2 days"]),
        sink=Sink(
            writers=[
                HistoricalFeatureStoreWriter(db_config=MetastoreConfig(format_="DELTA"))
            ]
        ),
    )

    # act
    result_metadata = pipeline.build_metadata()

    # assert
    feature_set_metadata = pipeline.feature_set.build_metadata()
    readers_metadata = [reader.build_metadata() for reader in pipeline.source.readers]
    writers_metadata = [writer.build_metadata() for writer in pipeline.sink.writers]
    expected_metadata = FeatureSetPipelineMetadata(
        feature_set=feature_set_metadata,
        readers=readers_metadata,
        writers=writers_metadata,
    )

    assert result_metadata == expected_metadata
