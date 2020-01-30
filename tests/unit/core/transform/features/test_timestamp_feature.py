from butterfree.core.constant.data_type import DataType
from butterfree.core.transform.features import TimestampFeature


class TestKeyFeature:
    def test_args_without_transformation(self):

        test_key = TimestampFeature(from_column="ts")

        assert test_key.name == "timestamp"
        assert test_key.from_column == "ts"
        assert test_key.description == "Records when any data for this feature set."
        assert test_key.dtype == DataType.TIMESTAMP
