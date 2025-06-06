import pyspark.sql.functions as F

from butterfree.constants import DataType
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import SparkFunctionTransform
from butterfree.transform.utils import Function


class TestFeatureSetMetadata:
    def test_build_metadata(
        self,
    ):
        # arrange
        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[KeyFeature(name="key", description="d", dtype=DataType.STRING)],
            timestamp=TimestampFeature(),
            features=[
                Feature(
                    name="feature",
                    description="d",
                    transformation=SparkFunctionTransform(
                        functions=[Function(F.avg, DataType.DOUBLE)]
                    ),
                ),
            ],
        )

        # act
        result = feature_set.build_metadata()

        # assert
        assert result.name == "name"
        assert result.entity == "entity"
        assert result.type == "FeatureSet"
        assert result.description == "description"

        assert len(result.features) == 3

        assert result.features[0].name == "key"
        assert result.features[0].primary_key is True

        assert result.features[1].name == "timestamp"
        assert result.features[1].primary_key is False

        assert result.features[2].name == "feature__avg"
        assert result.features[2].primary_key is False
