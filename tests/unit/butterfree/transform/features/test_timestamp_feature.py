from pyspark.sql.types import StringType

from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.transform.features import TimestampFeature


class TestTimestampFeature:
    def test_args_without_transformation(self):

        test_key = TimestampFeature(from_column="ts")

        assert test_key.name == TIMESTAMP_COLUMN
        assert test_key.from_column == "ts"
        assert test_key.dtype == DataType.TIMESTAMP

    def test_transform(self, feature_set_dataframe):

        test_key = TimestampFeature()

        df = test_key.transform(feature_set_dataframe)

        assert df.schema[TIMESTAMP_COLUMN].dataType == DataType.TIMESTAMP.spark

    def test_transform_ms_from_column(self, feature_set_dataframe_ms_from_column):

        test_key = TimestampFeature(from_column="ts", from_ms=True)

        df = test_key.transform(feature_set_dataframe_ms_from_column).orderBy(
            "timestamp"
        )

        df = df.withColumn("timestamp", df["timestamp"].cast(StringType())).collect()

        assert df[0]["timestamp"] == "2020-02-12 21:18:31"
        assert df[1]["timestamp"] == "2020-02-12 21:18:42"

    def test_transform_ms(self, feature_set_dataframe_ms):

        test_key = TimestampFeature(from_ms=True)

        df = test_key.transform(feature_set_dataframe_ms).orderBy("timestamp")

        df = df.withColumn("timestamp", df["timestamp"].cast(StringType())).collect()

        assert df[0]["timestamp"] == "2020-02-12 21:18:31"
        assert df[1]["timestamp"] == "2020-02-12 21:18:42"

    def test_transform_mask(self, feature_set_dataframe_date):

        test_key = TimestampFeature(mask="yyyy-MM-dd")

        df = test_key.transform(feature_set_dataframe_date).orderBy("timestamp")

        df = df.withColumn("timestamp", df["timestamp"].cast(StringType())).collect()

        assert df[0]["timestamp"] == "2020-02-07 00:00:00"
        assert df[1]["timestamp"] == "2020-02-08 00:00:00"
