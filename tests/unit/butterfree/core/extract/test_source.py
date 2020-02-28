from testing import check_dataframe_equality

from butterfree.core.clients import SparkClient
from butterfree.core.extract import Source


class TestSource:
    def test_construct(self, mocker, target_df):
        # given
        spark_client = SparkClient()

        reader_id = "a_source"
        reader = mocker.stub(reader_id)
        reader.build = mocker.stub("build")
        reader.build.side_effect = target_df.createOrReplaceTempView(reader_id)

        # when
        source_selector = Source(
            readers=[reader], query=f"select * from {reader_id}",  # noqa
        )

        output_df = source_selector.construct(spark_client)

        assert check_dataframe_equality(output_df, target_df)

    def test_is_cached(self, mocker, target_df):
        # given
        spark_client = SparkClient()

        reader_id = "a_source"
        reader = mocker.stub(reader_id)
        reader.build = mocker.stub("build")
        reader.build.side_effect = target_df.createOrReplaceTempView(reader_id)

        # when
        source_selector = Source(
            readers=[reader], query=f"select * from {reader_id}",  # noqa
        )

        output_df = source_selector.construct(spark_client)

        assert output_df.is_cached
