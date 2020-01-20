import pytest
from pyspark.sql.functions import expr

from butterfree.core.source import FileSource


def add_value_transformer(df, column, value):
    return df.withColumn(column, expr("{} + {}".format(column, value)))


def subtract_value_transformer(df, column, value):
    return df.withColumn(column, expr("{} - {}".format(column, value)))


class TestSource:
    @pytest.mark.parametrize(
        "transformations",
        [
            [
                {
                    "transformer": add_value_transformer,
                    "args": ("col1", 1000),
                    "kwargs": {},
                }
            ],
            [
                {
                    "transformer": subtract_value_transformer,
                    "args": ("col1", 1000),
                    "kwargs": {},
                }
            ],
            [
                {
                    "transformer": subtract_value_transformer,
                    "args": ("col1", 1000),
                    "kwargs": {},
                },
                {
                    "transformer": add_value_transformer,
                    "args": ("col1", 1000),
                    "kwargs": {},
                },
            ],
        ],
    )
    def test_with_(self, transformations, spark_client):
        # arrange
        file_source = FileSource("test", spark_client, "path/to/file", "format")

        # act
        for transformation in transformations:
            file_source.with_(
                transformation["transformer"],
                *transformation["args"],
                **transformation["kwargs"]
            )

        # assert
        assert file_source.transformations == transformations

    @pytest.mark.parametrize(
        "input_data, transformations, transformed_data",
        [
            (
                [{"col1": 100}],
                [
                    {
                        "transformer": add_value_transformer,
                        "args": ("col1", 1000),
                        "kwargs": {},
                    }
                ],
                [{"col1": 1100}],
            ),
            (
                [{"col1": 100}],
                [
                    {
                        "transformer": subtract_value_transformer,
                        "args": ("col1", 1000),
                        "kwargs": {},
                    }
                ],
                [{"col1": -900}],
            ),
            (
                [{"col1": 100}],
                [
                    {
                        "transformer": subtract_value_transformer,
                        "args": ("col1", 1000),
                        "kwargs": {},
                    },
                    {
                        "transformer": add_value_transformer,
                        "args": ("col1", 1000),
                        "kwargs": {},
                    },
                ],
                [{"col1": 100}],
            ),
        ],
    )
    def test__apply_transformations(
        self, input_data, transformations, transformed_data, sc, spark, spark_client,
    ):
        # arrange
        file_source = FileSource("test", spark_client, "path/to/file", "format")
        file_source.transformations = transformations
        input_df = spark.read.json(sc.parallelize(input_data, 1))
        target_df = spark.read.json(sc.parallelize(transformed_data, 1))

        # act
        result_df = file_source._apply_transformations(input_df)

        # assert
        assert target_df.collect() == result_df.collect()

    def test_build(self, target_df, spark_client, spark):
        # arrange
        file_source = FileSource("test", spark_client, "path/to/file", "format")
        spark_client.read.return_value = target_df

        # act
        file_source.build()
        result_df = spark.sql("select * from test")

        # assert
        assert target_df.collect() == result_df.collect()
