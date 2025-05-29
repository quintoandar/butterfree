"""Test feature with external window metadata generation."""

from pyspark.sql import functions as F

from butterfree.constants import DataType
from butterfree.metadata.feature_metadata import FeatureMetadata
from butterfree.transform import FeatureSet
from butterfree.transform.features import Feature
from butterfree.transform.features.key_feature import KeyFeature
from butterfree.transform.features.timestamp_feature import TimestampFeature
from butterfree.transform.transformations import SparkFunctionTransform
from butterfree.transform.utils import Function, Window


class TestFeatureExternalWindow:
    """Tests for Feature with external window handling."""

    def test_build_metadata_spark_function_with_internal_window_ignore_external_window(
        self, mocker
    ):
        """SparkFunctionTransform with internal window ignores external window."""  # noqa: E501

        # Create a SparkFunctionTransform with internal window
        transformation = SparkFunctionTransform(
            functions=[
                Function(func=F.avg, data_type=DataType.DOUBLE),
            ]
        )
        # Add internal windows
        transformation.with_window(
            partition_by="id",
            window_definition=["5 minutes"],
            mode="fixed_windows",
        )

        feature = Feature(
            name="feature_spark",
            description="spark function feature",
            transformation=transformation,
        )

        # Mock the get_output_columns to return a name that includes the internal window
        mocker.patch.object(
            feature,
            "get_output_columns",
            return_value=["feature_spark__avg_over_5_minutes_fixed_windows"],
        )

        # Create an external window that should be ignored
        external_window = Window(
            partition_by="id",
            mode="fixed_windows",
            window_definition="2 minutes",
        )

        # Build metadata with the external window
        metadata = feature.build_metadata(window=external_window)

        # Assert
        assert len(metadata) == 1
        assert (
            metadata[0].name == "feature_spark__avg_over_5_minutes_fixed_windows"
        ), "External window should be ignored when SparkFunctionTransform has internal windows"  # noqa: E501
        assert metadata[0].data_type == "DOUBLE"
        assert metadata[0].primary_key is False
        assert metadata[0].description == "spark function feature"

    def test_feature_set_build_metadata_with_spark_function_transform(self, mocker):
        """Test FeatureSet._build_features_metadata correctly handles SparkFunctionTransform."""  # noqa: E501

        key_id = KeyFeature(name="id", description="id key", dtype=DataType.INTEGER)
        timestamp_c = TimestampFeature()

        # Create a feature with SparkFunctionTransform with internal windows
        feature_with_internal_window = Feature(
            name="feature_with_internal",
            description="feature with internal window",
            transformation=SparkFunctionTransform(
                functions=[
                    Function(func=F.avg, data_type=DataType.DOUBLE),
                ]
            ).with_window(
                partition_by="id",
                window_definition=["5 minutes"],
                mode="fixed_windows",
            ),
        )

        # Mock build_metadata to return expected result
        mocker.patch.object(
            feature_with_internal_window,
            "build_metadata",
            return_value=[
                FeatureMetadata(
                    name="feature_with_internal__avg_over_5_minutes_fixed_windows",
                    data_type="DOUBLE",
                    description="feature with internal window",
                    primary_key=False,
                )
            ],
        )

        # Create a feature with SparkFunctionTransform without internal windows
        feature_without_internal_window = Feature(
            name="feature_without_internal",
            description="feature without internal window",
            transformation=SparkFunctionTransform(
                functions=[Function(func=F.sum, data_type=DataType.DOUBLE)]
            ),
        )

        # Mock build_metadata to return expected result
        mocker.patch.object(
            feature_without_internal_window,
            "build_metadata",
            return_value=[
                FeatureMetadata(
                    name="feature_without_internal__sum",
                    data_type="DOUBLE",
                    description="feature without internal window",
                    primary_key=False,
                )
            ],
        )

        # Create the feature set
        feature_set = FeatureSet(
            name="name",
            entity="entity",
            description="description",
            keys=[key_id],
            timestamp=timestamp_c,
            features=[feature_with_internal_window, feature_without_internal_window],
        )

        # Build metadata
        metadata = feature_set._build_features_metadata()

        # Assert
        assert len(metadata) == 2
        feature_with_internal_window.build_metadata.assert_called_once()
        feature_without_internal_window.build_metadata.assert_called_once()

        # Convert to a dictionary for easier lookup by name
        metadata_dict = {m.name: m for m in metadata}

        # Verify both features are included
        assert (
            "feature_with_internal__avg_over_5_minutes_fixed_windows" in metadata_dict
        )
        assert "feature_without_internal__sum" in metadata_dict
