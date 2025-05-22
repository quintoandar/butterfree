import pytest
from pydantic import ValidationError

from butterfree.constants import DataType
from butterfree.pipelines.feature_set_pipeline_metadata import Catalog, Column


class TestColumn:
    def test_create_valid_column(self):
        # Given
        name = "user_id"
        data_type = DataType.STRING.name
        primary_key = True
        description = "test"
        # When
        column = Column(
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
            Column(
                data_type=DataType.STRING,
                primary_key=True,
            )
        assert "name" in str(exc_info.value)

        # Test missing data_type
        with pytest.raises(ValidationError) as exc_info:
            Column(
                name="user_id",
                primary_key=True,
            )
        assert "data_type" in str(exc_info.value)

        # Test missing primary_key
        with pytest.raises(ValidationError) as exc_info:
            Column(
                name="user_id",
                data_type=DataType.STRING,
            )
        assert "primary_key" in str(exc_info.value)

        # Test missing description
        with pytest.raises(ValidationError) as exc_info:
            Column(
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
            Column(
                name=name,
                data_type=data_type,
                primary_key=primary_key,
                description=description,
            )
        assert "data_type" in str(exc_info.value)


class TestCatalog:
    def test_create_valid_catalog(self):
        # Given
        name = "user_features"
        description = "User related features"
        columns = [
            Column(
                name="user_id",
                data_type=DataType.STRING.name,
                primary_key=True,
                description="test",
            ),
            Column(
                name="age",
                data_type=DataType.INTEGER.name,
                primary_key=False,
                description="test",
            ),
            Column(
                name="name",
                data_type=DataType.STRING.name,
                primary_key=False,
                description="test",
            ),
        ]

        # When
        catalog = Catalog(
            feature_set_name=name,
            description=description,
            columns=columns,
        )

        # Then
        assert catalog.feature_set_name == name
        assert catalog.description == description
        assert len(catalog.columns) == 3
        assert catalog.columns[0].name == "user_id"
        assert catalog.columns[0].data_type == DataType.STRING.name
        assert catalog.columns[0].primary_key is True
        assert catalog.columns[1].name == "age"
        assert catalog.columns[1].data_type == DataType.INTEGER.name
        assert catalog.columns[1].primary_key is False
        assert catalog.columns[1].description == "test"
        assert catalog.columns[2].name == "name"
        assert catalog.columns[2].data_type == DataType.STRING.name
        assert catalog.columns[2].primary_key is False
        assert catalog.columns[2].description == "test"

    def test_create_catalog_with_missing_required_fields(self):
        # Test missing name
        with pytest.raises(ValidationError) as exc_info:
            Catalog(
                description="User related features",
                columns=[
                    Column(
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
            Catalog(
                feature_set_name="user_features",
                columns=[
                    Column(
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
            Catalog(
                feature_set_name="user_features",
                description="User related features",
            )
        assert "columns" in str(exc_info.value)

    def test_create_catalog_with_invalid_column(self):
        # Given
        name = "user_features"
        description = "User related features"
        columns = [
            Column(
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
            Catalog(
                feature_set_name=name,
                description=description,
                columns=columns,
            )
        assert "columns" in str(exc_info.value)
