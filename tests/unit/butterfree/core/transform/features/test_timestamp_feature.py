from datetime import datetime

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.features import TimestampFeature


class TestTimestampFeature:
    def test_args_without_transformation(self):

        test_key = TimestampFeature(from_column="ts")

        assert test_key.name == TIMESTAMP_COLUMN
        assert test_key.from_column == "ts"
        assert test_key.dtype == DataType.TIMESTAMP

    def test_transform(self, feature_set_dataframe):

        test_key = TimestampFeature()

        df = test_key.transform(feature_set_dataframe)

        assert df.schema[TIMESTAMP_COLUMN].dataType == DataType.TIMESTAMP.value

    def test_transform_ms_from_column(self, feature_set_dataframe_ms_from_column):

        test_key = TimestampFeature(from_column="ts", from_ms=True)

        df = (
            test_key.transform(feature_set_dataframe_ms_from_column)
            .orderBy("timestamp")
            .collect()
        )

        assert df[0]["timestamp"] == datetime(2020, 2, 12, 18, 18, 31)
        assert df[1]["timestamp"] == datetime(2020, 2, 12, 18, 18, 42)

    def test_transform_ms(self, feature_set_dataframe_ms):

        test_key = TimestampFeature(from_ms=True)

        df = test_key.transform(feature_set_dataframe_ms).orderBy("timestamp").collect()

        assert df[0]["timestamp"] == datetime(2020, 2, 12, 18, 18, 31)
        assert df[1]["timestamp"] == datetime(2020, 2, 12, 18, 18, 42)

    def test_transform_mask(self, feature_set_dataframe_date):

        test_key = TimestampFeature(mask="yyyy-MM-dd")

        df = (
            test_key.transform(feature_set_dataframe_date)
            .orderBy("timestamp")
            .collect()
        )

        assert df[0]["timestamp"] == datetime(2020, 2, 7, 0, 0, 0)
        assert df[1]["timestamp"] == datetime(2020, 2, 8, 0, 0, 0)
