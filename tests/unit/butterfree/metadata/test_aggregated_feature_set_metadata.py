import pyspark.sql.functions as F

from butterfree.constants import DataType
from butterfree.transform.aggregated_feature_set import AggregatedFeatureSet
from butterfree.transform.features import Feature, KeyFeature, TimestampFeature
from butterfree.transform.transformations import AggregatedTransform
from butterfree.transform.utils import Function


class TestAggregatedFeatureSetMetadata:
    def test_build_metadata(self):
        # arrange
        feature_set = AggregatedFeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[KeyFeature(name="key", description="d", dtype=DataType.STRING)],
            timestamp=TimestampFeature(from_column="timestamp"),
            features=[
                Feature(
                    name="feature",
                    description="d",
                    from_column="value",
                    transformation=AggregatedTransform(
                        functions=[Function(F.avg, DataType.DOUBLE)]
                    ),
                )
            ],
        )
        feature_set = feature_set.with_windows(["3 days"]).with_pivot(
            column="pivot_column", values=["a"]
        )

        # act
        result = feature_set.build_metadata()

        # assert
        assert result.name == "name"
        assert result.entity == "entity"
        assert result.type == "AggregatedFeatureSet"
        assert result.description == "description"

        assert len(result.windows_definition) == 1
        assert result.windows_definition[0] == "3 days"

        assert len(result.features) == 3

        assert result.features[0].name == "key"
        assert result.features[0].description == "d"
        assert result.features[0].primary_key is True

        assert result.features[1].name == "timestamp"
        assert (
            result.features[1].description == "Time tag for the state of all features."
        )
        assert result.features[1].primary_key is False

        assert result.features[2].name == "a_feature__avg_over_3_days_rolling_windows"
        assert result.features[2].description == "d"
        assert result.features[2].primary_key is False
