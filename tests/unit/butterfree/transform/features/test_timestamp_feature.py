from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType

from butterfree.clients import SparkClient
from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.transform.features import TimestampFeature

# from pyspark.sql.types import *


class TestTimestampFeature:
    def test_args_without_transformation(self):

        test_key = TimestampFeature(from_column="ts")
        test_key_ntz = TimestampFeature(dtype=DataType.TIMESTAMP_NTZ, from_column="ts")

        assert test_key.name == TIMESTAMP_COLUMN
        assert test_key.from_column == "ts"
        assert test_key.dtype == DataType.TIMESTAMP
        assert test_key_ntz.dtype == DataType.TIMESTAMP_NTZ

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

        assert df[0]["timestamp"] == "2020-02-12 21:18:31.112"
        assert df[1]["timestamp"] == "2020-02-12 21:18:42.223"

    def test_transform_ms(self, feature_set_dataframe_ms):

        test_key = TimestampFeature(from_ms=True)

        df = test_key.transform(feature_set_dataframe_ms).orderBy("timestamp")

        df = df.withColumn("timestamp", df["timestamp"].cast(StringType())).collect()

        assert df[0]["timestamp"] == "2020-02-12 21:18:31.112"
        assert df[1]["timestamp"] == "2020-02-12 21:18:42.223"

    def test_transform_ms_from_column_small_time_diff(
        self, feature_set_dataframe_small_time_diff
    ):

        test_key = TimestampFeature(from_ms=True)

        df = test_key.transform(feature_set_dataframe_small_time_diff).orderBy(
            "timestamp"
        )

        df = df.withColumn("timestamp", df["timestamp"].cast(StringType())).collect()

        assert df[0]["timestamp"] != df[1]["timestamp"]

    def test_transform_mask(self, feature_set_dataframe_date):

        test_key = TimestampFeature(mask="yyyy-MM-dd")

        df = test_key.transform(feature_set_dataframe_date).orderBy("timestamp")

        df = df.withColumn("timestamp", df["timestamp"].cast(StringType())).collect()

        assert df[0]["timestamp"] == "2020-02-07 00:00:00"
        assert df[1]["timestamp"] == "2020-02-08 00:00:00"

    def test_no_timezone(self):

        spark = SparkClient()
        now = datetime.now()

        # Testing a new timezone
        spark.conn.conf.set("spark.sql.session.timeZone", "GMT-5")

        l = [(now, now)]
        rdd = spark.conn.sparkContext.parallelize(l)

        schema = StructType(
            [
                StructField("ts", DataType.TIMESTAMP.spark, True),
                StructField("ts_ntz", DataType.TIMESTAMP_NTZ.spark, True),
            ]
        )
        df = spark.conn.createDataFrame(rdd, schema)
        df.createOrReplaceTempView("temp_tz_table")

        df1 = spark.conn.sql("""SELECT ts, ts_ntz FROM temp_tz_table""")
        df2 = df1.withColumns(
            {"ts": df1.ts.cast(StringType()), "ts_ntz": df1.ts_ntz.cast(StringType())}
        )
        df2_vals = df2.collect()[0]

        assert df2_vals.ts != df2_vals.ts_ntz

        # New timezone, the column with TZ must have a different value, and the column NTZ must keep its value
        spark.conn.conf.set("spark.sql.session.timeZone", "GMT-7")

        df3 = spark.conn.sql("""SELECT ts, ts_ntz FROM temp_tz_table""")
        df4 = df3.withColumns(
            {"ts": df1.ts.cast(StringType()), "ts_ntz": df1.ts_ntz.cast(StringType())}
        )
        df4_vals = df4.collect()[0]

        assert df4_vals.ts != df2_vals.ts
        assert df4_vals.ts_ntz == df2_vals.ts_ntz
