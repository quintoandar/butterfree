from functools import partial

from pyspark.sql import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.reader import Source


def create_temp_view(dataframe: DataFrame, name):
    dataframe.createOrReplaceTempView(name)


class TestSource:
    def test_construct(self, mocker, target_df):
        # given
        spark_client = SparkClient()

        reader_id = "a_source"
        reader = mocker.stub(reader_id)
        reader.build = mocker.stub("build")
        reader.build.side_effect = partial(create_temp_view, target_df, reader_id)

        # when
        source_selector = Source(
            readers=[reader], query=f"select * from {reader_id}",  # noqa
        )

        result_df = source_selector.construct(spark_client)

        assert result_df.collect() == target_df.collect()

    def test_is_cached(self, mocker, target_df):
        # given
        spark_client = SparkClient()

        reader_id = "a_source"
        reader = mocker.stub(reader_id)
        reader.build = mocker.stub("build")
        reader.build.side_effect = partial(create_temp_view, target_df, reader_id)

        # when
        source_selector = Source(
            readers=[reader], query=f"select * from {reader_id}",  # noqa
        )

        result_df = source_selector.construct(spark_client)

        assert result_df.is_cached
