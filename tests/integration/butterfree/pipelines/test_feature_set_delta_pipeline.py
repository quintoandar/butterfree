from unittest import mock

import pytest

from butterfree.clients import SparkClient
from butterfree.constants.data_type import DataType
from butterfree.extract import Source
from butterfree.extract.readers import TableReader
from butterfree.load import Sink
from butterfree.load.writers import DeltaFeatureStoreWriter
from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature


@pytest.fixture
def spark_client():
    return SparkClient()


class TestFeatureSetPipeline:
    def test_feature_set_pipeline_with_delta_writer(self, spark_client):
        # given
        # Criar DataFrame de exemplo usando o spark_client
        df_data = [(1, 1.0, "2021-01-01")]  # id, feature, timestamp
        dataframe = spark_client.conn.createDataFrame(
            df_data, ["id", "feature", "timestamp"]
        )

        pipeline = FeatureSetPipeline(
            source=Source(
                readers=[
                    TableReader(
                        id="id",
                        database="db",
                        table="table",
                    ),
                ],
                query="select * from id",
            ),
            feature_set=FeatureSet(
                name="feature_set",
                entity="entity",
                description="description",
                features=[
                    Feature(name="feature", description="test", dtype=DataType.FLOAT)
                ],
                keys=[KeyFeature(name="id", description="id", dtype=DataType.INTEGER)],
                timestamp=TimestampFeature(),
                deduplicate_rows=True,
            ),
            sink=Sink(
                writers=[
                    DeltaFeatureStoreWriter(
                        database="test_db",
                        table="test_table",
                        merge_on=["id"],
                    )
                ]
            ),
            spark_client=spark_client,
        )

        # Mock usando o DataFrame real
        pipeline.source.construct = mock.Mock(return_value=dataframe)
        pipeline.feature_set.construct = mock.Mock(return_value=dataframe)

        # Mock o DeltaWriter.merge
        with mock.patch(
            "butterfree.load.writers.delta_writer.DeltaWriter.merge"
        ) as mock_merge:
            # when
            pipeline.run()

            # then
            pipeline.source.construct.assert_called_once()
            pipeline.feature_set.construct.assert_called_once()
            mock_merge.assert_called_once()
