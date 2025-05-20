import pytest
from pydantic import ValidationError

from butterfree.constants import DataType
from butterfree.pipelines.feature_set_pipeline_metadata import Catalog, Column


class TestColumn:
    def test_create_valid_column(self):
        # Given
        name = "user_id"
        data_type = DataType.STRING
        primary_key = True

        # When
        column = Column(
            name=name,
            data_type=data_type,
            primary_key=primary_key,
        )

        # Then
        assert column.name == name
        assert column.data_type == data_type
        assert column.primary_key == primary_key

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

    def test_create_column_with_invalid_data_type(self):
        # Given
        name = "user_id"
        data_type = "invalid_type"
        primary_key = True

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            Column(
                name=name,
                data_type=data_type,
                primary_key=primary_key,
            )
        assert "data_type" in str(exc_info.value)


class TestCatalog:
    def test_create_valid_catalog(self):
        # Given
        name = "user_features"
        description = "User related features"
        columns = [
            Column(name="user_id", data_type=DataType.STRING, primary_key=True),
            Column(name="age", data_type=DataType.INTEGER, primary_key=False),
            Column(name="name", data_type=DataType.STRING, primary_key=False),
        ]

        # When
        catalog = Catalog(
            name=name,
            description=description,
            columns=columns,
        )

        # Then
        assert catalog.name == name
        assert catalog.description == description
        assert len(catalog.columns) == 3
        assert catalog.columns[0].name == "user_id"
        assert catalog.columns[0].data_type == DataType.STRING
        assert catalog.columns[0].primary_key is True
        assert catalog.columns[1].name == "age"
        assert catalog.columns[1].data_type == DataType.INTEGER
        assert catalog.columns[1].primary_key is False

    def test_create_catalog_with_missing_required_fields(self):
        # Test missing name
        with pytest.raises(ValidationError) as exc_info:
            Catalog(
                description="User related features",
                columns=[
                    Column(name="user_id", data_type=DataType.STRING, primary_key=True)
                ],
            )
        assert "name" in str(exc_info.value)

        # Test missing description
        with pytest.raises(ValidationError) as exc_info:
            Catalog(
                name="user_features",
                columns=[
                    Column(name="user_id", data_type=DataType.STRING, primary_key=True)
                ],
            )
        assert "description" in str(exc_info.value)

        # Test missing columns
        with pytest.raises(ValidationError) as exc_info:
            Catalog(
                name="user_features",
                description="User related features",
            )
        assert "columns" in str(exc_info.value)

    def test_create_catalog_with_invalid_column(self):
        # Given
        name = "user_features"
        description = "User related features"
        columns = [
            Column(name="user_id", data_type=DataType.STRING, primary_key=True),
            {
                "name": "invalid_column",
                "data_type": "invalid_type",
                "primary_key": False,
            },  # Invalid column format
        ]

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            Catalog(
                name=name,
                description=description,
                columns=columns,
            )
        assert "columns" in str(exc_info.value)
