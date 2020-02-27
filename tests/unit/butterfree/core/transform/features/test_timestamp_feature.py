from testing import compare_dataframes

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.constants.data_type import DataType
from butterfree.core.transform.features import TimestampFeature


class TestTimestampFeature:
    def test_args_without_transformation(self):

        test_key = TimestampFeature(from_column="ts")

        assert test_key.name == TIMESTAMP_COLUMN
        assert test_key.from_column == "ts"
        assert test_key.dtype == DataType.TIMESTAMP

    def test_transform(self, target_df):

        test_key = TimestampFeature()

        output_df = test_key.transform(target_df)

        assert output_df.schema[TIMESTAMP_COLUMN].dataType == DataType.TIMESTAMP.value

    def test_transform_ms_from_column(self, input_ms_from_column, target_df):

        test_key = TimestampFeature(from_column="ts", from_ms=True)

        output_df = test_key.transform(input_ms_from_column).orderBy("timestamp")

        assert compare_dataframes(output_df, target_df)

    def test_transform_ms(self, input_ms, target_df):

        test_key = TimestampFeature(from_ms=True)

        output_df = test_key.transform(input_ms)

        assert compare_dataframes(output_df, target_df)

    def test_transform_mask(self, input_date, date_target_df):

        test_key = TimestampFeature(mask="yyyy-MM-dd")

        output_df = test_key.transform(input_date).orderBy("timestamp")

        assert compare_dataframes(output_df, date_target_df)
