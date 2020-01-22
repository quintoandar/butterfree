from functools import partial

from pyspark.sql import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.source import SourceSelector


def create_temp_view(dataframe: DataFrame, name):
    dataframe.createOrReplaceTempView(name)


class TestSourceSelector:
    def test_construct(self, mocker, target_df):
        # given
        spark_client = SparkClient()

        source_id = "a_source"
        source = mocker.stub(source_id)
        source.build = mocker.stub("build")
        source.build.side_effect = partial(create_temp_view, target_df, source_id)

        # when
        source_selector = SourceSelector(
            spark_client=spark_client,
            sources=[source],
            query=f"select * from {source_id}",  # noqa
        )

        result_df = source_selector.construct()

        assert result_df.collect() == target_df.collect()
