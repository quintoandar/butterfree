import pytest
from pydantic import ValidationError

from butterfree.constants import DataType
from butterfree.metadata.feature_metadata import FeatureMetadata


class TestFeatureMetadata:
    def test_create_valid_column(self):
        # Given
        name = "user_id"
        data_type = DataType.STRING.name
        primary_key = True
        description = "test"
        # When
        column = FeatureMetadata(
            name=name,
            data_type=data_type,
            primary_key=primary_key,
            description=description,
        )

        # Then
        assert column.name == name
        assert column.data_type == data_type
        assert column.primary_key == primary_key
        assert column.description == "test"

    def test_create_column_with_missing_required_fields(self):
        # Test missing name
        with pytest.raises(ValidationError) as exc_info:
            FeatureMetadata(
                data_type=DataType.STRING,
                primary_key=True,
            )
        assert "name" in str(exc_info.value)

        # Test missing data_type
        with pytest.raises(ValidationError) as exc_info:
            FeatureMetadata(
                name="user_id",
                primary_key=True,
            )
        assert "data_type" in str(exc_info.value)

        # Test missing primary_key
        with pytest.raises(ValidationError) as exc_info:
            FeatureMetadata(
                name="user_id",
                data_type=DataType.STRING,
            )
        assert "primary_key" in str(exc_info.value)

        # Test missing description
        with pytest.raises(ValidationError) as exc_info:
            FeatureMetadata(
                name="user_id",
                data_type=DataType.STRING,
                primary_key=True,
            )
        assert "description" in str(exc_info.value)

    def test_create_column_with_invalid_data_type(self):
        # Given
        name = "user_id"
        data_type = 1
        primary_key = True
        description = "test"

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            FeatureMetadata(
                name=name,
                data_type=data_type,
                primary_key=primary_key,
                description=description,
            )
        assert "data_type" in str(exc_info.value)
