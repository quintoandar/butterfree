import pytest
from pyspark.sql import functions

from butterfree.core.clients import SparkClient
from butterfree.core.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import (
    AggregatedTransform,
    SparkFunctionTransform,
)


class TestFeatureSet:
    def test_feature_set_with_invalid_feature(self, key_id, timestamp_c, dataframe):
        spark_client = SparkClient()

        with pytest.raises(ValueError):
            AggregatedFeatureSet(
                name="name",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature1",
                        description="test",
                        transformation=SparkFunctionTransform(
                            functions=[functions.avg],
                        ).with_window(
                            partition_by="id",
                            mode="row_windows",
                            window_definition=["2 events"],
                        ),
                    ),
                ],
                keys=[key_id],
                timestamp=timestamp_c,
            ).construct(dataframe, spark_client)

    def test_feature_set_with_window_and_no_window(
        self, key_id, timestamp_c, dataframe
    ):
        spark_client = SparkClient()

        with pytest.raises(ValueError):
            AggregatedFeatureSet(
                name="name",
                entity="entity",
                description="description",
                features=[
                    Feature(
                        name="feature1",
                        description="unit test",
                        transformation=AggregatedTransform(
                            functions=["avg"], group_by="id", column="feature1",
                        ).with_window(window_definition=["1 day", "1 week"]),
                    ),
                    Feature(
                        name="feature1",
                        description="unit test",
                        transformation=AggregatedTransform(
                            functions=["count"], group_by="id", column="feature1",
                        ),
                    ),
                ],
                keys=[key_id],
                timestamp=timestamp_c,
            ).construct(dataframe, spark_client)
