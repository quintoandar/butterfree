from pyspark.sql.functions import spark_partition_id

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

        result_df = source_selector.construct(spark_client)

        assert result_df.collect() == target_df.collect()

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

        result_df = source_selector.construct(spark_client)

        assert result_df.is_cached

    def test_construct_with_repartition(self, mocker, feature_set_dataframe):
        # given
        spark_client = SparkClient()

        reader_id = "a_source"
        reader = mocker.stub(reader_id)
        reader.build = mocker.stub("build")
        reader.build.side_effect = feature_set_dataframe.createOrReplaceTempView(
            reader_id
        )

        # when
        source_selector = Source(
            readers=[reader], query=f"select * from {reader_id}",  # noqa
        )

        result_df = source_selector.construct(
            spark_client, partition_by=["id"], num_partitions=2
        )

        # assert
        assert result_df.is_cached
        assert sorted(result_df.collect()) == sorted(feature_set_dataframe.collect())
        # Only one partition id, meaning data is not partitioned
        assert (
            feature_set_dataframe.select(spark_partition_id()).distinct().count() == 1
        )
        # Desired number of partitions
        assert result_df.select(spark_partition_id()).distinct().count() == 2
