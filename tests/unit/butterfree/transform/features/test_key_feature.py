from unittest.mock import Mock

from butterfree.constants import DataType
from butterfree.transform.features import KeyFeature


class TestKeyFeature:
    def test_args_without_transformation(self):

        test_key = KeyFeature(
            name="id",
            from_column="origin",
            description="unit test",
            dtype=DataType.INTEGER,
        )

        assert test_key.name == "id"
        assert test_key.from_column == "origin"
        assert test_key.description == "unit test"

    def test_args_with_transformation(self):

        test_key = KeyFeature(
            name="id",
            from_column="origin",
            description="unit test",
            dtype=DataType.INTEGER,
            transformation=Mock(),
        )
        assert test_key.name == "id"
        assert test_key.from_column == "origin"
        assert test_key.description == "unit test"
        assert test_key.transformation
