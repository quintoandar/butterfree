import pytest
from pyspark.sql.functions import expr
from functools import reduce

from butterfree.core.source import FileSource


def add_value_transformer(df, column, value):
    return df.withColumn(column, expr("{} + {}".format(column, value)))


def subtract_value_transformer(df, column, value):
    return df.withColumn(column, expr("{} - {}".format(column, value)))


class TestSource:
    @pytest.mark.parametrize(
        "with_transformations",
        [
            [{"transformer": add_value_transformer, "args": ("col2", 1000)},],
            [{"transformer": subtract_value_transformer, "args": ("col2", 1000)},],
            [
                {"transformer": subtract_value_transformer, "args": ("col2", 1000)},
                {"transformer": add_value_transformer, "args": ("col2", 1000)},
            ],
        ],
    )
    def test_with_(self, spark_client, target_df, with_transformations):
        # arrange
        print("with_transformations:", with_transformations)
        spark_client.load.return_value = target_df
        file_source = FileSource(id="test", client=spark_client)

        # act
        result_file_source = reduce(  # apply transformations using with_ method
            lambda result_file_source, with_transformation: result_file_source.with_(
                with_transformation["transformer"], *with_transformation["args"]
            ),
            with_transformations,
            file_source,
        )
        print("result_file_source transformations:", result_file_source.transformations)
        result_df = result_file_source._apply_transformations(target_df)

        target_result_df = reduce(  # apply transformations manually
            lambda df, with_transformation: with_transformation["transformer"](
                df, *with_transformation["args"]
            ),
            with_transformations,
            target_df,
        )

        # assert
        assert target_result_df.collect() == result_df.collect()

    def test_build(self, target_df, spark_client, spark):
        # arrange
        file_source = FileSource(
            id="test",
            client=spark_client,
            consume_options={"path": "path/to/file.json", "format": "json"},
        )
        spark_client.load.return_value = target_df

        # act
        file_source.build()
        result_df = spark.sql("select * from test")

        # assert
        assert target_df.collect() == result_df.collect()
