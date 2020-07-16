from pyspark.sql.functions import spark_partition_id

from butterfree.dataframe_service import repartition_df, repartition_sort_df


class TestRepartition:
    def test_repartition_df(self, input_df):
        result_df = repartition_df(dataframe=input_df, partition_by=["timestamp"])

        # Only one partition id, meaning data is not partitioned
        assert input_df.select(spark_partition_id()).distinct().count() == 1
        # Desired number of partitions
        assert result_df.select(spark_partition_id()).distinct().count() == 200

    def test_repartition_df_partitions(self, input_df):
        result_df = repartition_df(
            dataframe=input_df, partition_by=["timestamp"], num_partitions=50
        )

        # Only one partition id, meaning data is not partitioned
        assert input_df.select(spark_partition_id()).distinct().count() == 1
        # Desired number of partitions
        assert result_df.select(spark_partition_id()).distinct().count() == 50

    def test_repartition_sort_df(self, input_df):
        result_df = repartition_sort_df(
            dataframe=input_df, partition_by=["timestamp"], order_by=["timestamp"]
        )

        # Only one partition id, meaning data is not partitioned
        assert input_df.select(spark_partition_id()).distinct().count() == 1
        # Desired number of partitions
        assert result_df.select(spark_partition_id()).distinct().count() == 200

    def test_repartition_sort_df_processors(self, input_df):
        result_df = repartition_sort_df(
            dataframe=input_df,
            partition_by=["timestamp"],
            order_by=["timestamp"],
            num_processors=3,
        )

        # Only one partition id, meaning data is not partitioned
        assert input_df.select(spark_partition_id()).distinct().count() == 1
        # Desired number of partitions
        assert result_df.select(spark_partition_id()).distinct().count() == 12

    def test_repartition_sort_df_processors_partitions(self, input_df):
        result_df = repartition_sort_df(
            dataframe=input_df,
            partition_by=["timestamp"],
            order_by=["timestamp"],
            num_partitions=50,
        )

        # Only one partition id, meaning data is not partitioned
        assert input_df.select(spark_partition_id()).distinct().count() == 1
        # Desired number of partitions
        assert result_df.select(spark_partition_id()).distinct().count() == 50
