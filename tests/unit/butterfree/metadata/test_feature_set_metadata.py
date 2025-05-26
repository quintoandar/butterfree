import pytest
from pydantic import ValidationError

from butterfree.constants import DataType
from butterfree.metadata.feature_metadata import FeatureMetadata
from butterfree.metadata.feature_set_metadata import FeatureSetMetadata


class TestFeatureSetMetadata:
    def test_create_valid_catalog(self):
        # Given
        name = "user_features"
        description = "User related features"
        columns = [
            FeatureMetadata(
                name="user_id",
                data_type=DataType.STRING.name,
                primary_key=True,
                description="test",
            ),
            FeatureMetadata(
                name="age",
                data_type=DataType.INTEGER.name,
                primary_key=False,
                description="test",
            ),
            FeatureMetadata(
                name="name",
                data_type=DataType.STRING.name,
                primary_key=False,
                description="test",
            ),
        ]

        # When
        feature_set_metadata = FeatureSetMetadata(
            name=name,
            description=description,
            features=columns,
        )

        # Then
        assert feature_set_metadata.name == name
        assert feature_set_metadata.description == description
        assert len(feature_set_metadata.features) == 3
        assert feature_set_metadata.features[0].name == "user_id"
        assert feature_set_metadata.features[0].data_type == DataType.STRING.name
        assert feature_set_metadata.features[0].primary_key is True
        assert feature_set_metadata.features[1].name == "age"
        assert feature_set_metadata.features[1].data_type == DataType.INTEGER.name
        assert feature_set_metadata.features[1].primary_key is False
        assert feature_set_metadata.features[1].description == "test"
        assert feature_set_metadata.features[2].name == "name"
        assert feature_set_metadata.features[2].data_type == DataType.STRING.name
        assert feature_set_metadata.features[2].primary_key is False
        assert feature_set_metadata.features[2].description == "test"

    def test_create_catalog_with_missing_required_fields(self):
        # Test missing name
        with pytest.raises(ValidationError) as exc_info:
            FeatureSetMetadata(
                description="User related features",
                features=[
                    FeatureMetadata(
                        name="user_id",
                        data_type=DataType.STRING,
                        primary_key=True,
                        description="test",
                    )
                ],
            )
        assert "name" in str(exc_info.value)

        # Test missing description
        with pytest.raises(ValidationError) as exc_info:
            FeatureSetMetadata(
                name="user_features",
                features=[
                    FeatureMetadata(
                        name="user_id",
                        data_type=DataType.STRING,
                        primary_key=True,
                        description="test",
                    )
                ],
            )
        assert "description" in str(exc_info.value)

        # Test missing columns
        with pytest.raises(ValidationError) as exc_info:
            FeatureSetMetadata(
                name="user_features",
                description="User related features",
            )
        assert "columns" in str(exc_info.value)

    def test_create_catalog_with_invalid_column(self):
        # Given
        name = "user_features"
        description = "User related features"
        columns = [
            FeatureMetadata(
                name="user_id",
                data_type=DataType.STRING,
                primary_key=True,
                description="test",
            ),
            {
                "name": "invalid_column",
                "data_type": "invalid_type",
                "primary_key": False,
            },  # Invalid column format
        ]

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            FeatureSetMetadata(
                name=name,
                description=description,
                features=columns,
            )
        assert "columns" in str(exc_info.value)
