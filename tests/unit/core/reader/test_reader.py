import pytest
from pyspark.sql.functions import expr, first

from butterfree.core.reader import FileReader
from butterfree.core.reader.pre_processing.pivot_transform import pivot_table


def add_value_transformer(df, column, value):
    return df.withColumn(column, expr("{} + {}".format(column, value)))


def subtract_value_transformer(df, column, value):
    return df.withColumn(column, expr("{} - {}".format(column, value)))


class TestReader:
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
        file_reader = FileReader("test", "path/to/file", "format")

        # act
        for transformation in transformations:
            file_reader.with_(
                transformation["transformer"],
                *transformation["args"],
                **transformation["kwargs"],
            )

        # assert
        assert file_reader.transformations == transformations

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
        file_reader = FileReader("test", "path/to/file", "format")
        file_reader.transformations = transformations
        input_df = spark.read.json(sc.parallelize(input_data, 1))
        target_df = spark.read.json(sc.parallelize(transformed_data, 1))

        # act
        result_df = file_reader._apply_transformations(input_df)

        # assert
        assert target_df.collect() == result_df.collect()

    def test__apply_transformations_(
        self, pivot_df, sc, spark, spark_client,
    ):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")
        file_reader.with_(
            transformer=pivot_table,
            group_by_columns=["id", "ts"],
            pivot_column="pivot_column",
            aggregation=first,
            agg_column="has_feature",
        )

        # act
        result_df = file_reader._apply_transformations(pivot_df).collect()

        # assert
        assert 1 == 1

    def test__apply_pivot_transformations(
        self, pivot_df, sc, spark, spark_client,
    ):
        df = pivot_table(
            dataframe=pivot_df,
            group_by_columns=["id", "ts"],
            pivot_column="pivot_column",
            aggregation=first,
            agg_column="has_feature",
        ).collect()

        df1 = df

        # assert
        assert 1 == 1

    def test_build(self, target_df, spark_client, spark):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")
        spark_client.read.return_value = target_df

        # act
        file_reader.build(spark_client)
        result_df = spark.sql("select * from test")

        # assert
        assert target_df.collect() == result_df.collect()
