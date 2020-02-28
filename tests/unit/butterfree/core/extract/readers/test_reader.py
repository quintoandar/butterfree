import json

import pytest
from pyspark.sql.functions import expr
from testing import check_dataframe_equality

from butterfree.core.extract.readers import FileReader


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
        self,
        input_data,
        transformations,
        transformed_data,
        spark_context,
        spark_session,
        spark_client,
    ):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")
        file_reader.transformations = transformations
        input_df = spark_session.read.json(
            spark_context.parallelize(input_data).map(lambda x: json.dumps(x))
        )
        target_df = spark_session.read.json(
            spark_context.parallelize(transformed_data).map(lambda x: json.dumps(x))
        )

        # act
        output_df = file_reader._apply_transformations(input_df)

        # assert
        assert check_dataframe_equality(output_df, target_df)

    def test_build(self, target_df, spark_client, spark_session):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")
        spark_client.read.return_value = target_df

        # act
        file_reader.build(spark_client)
        output_df = spark_session.sql("select * from test")

        # assert
        assert check_dataframe_equality(output_df, target_df)

    def test_build_with_columns(
        self, target_df, column_target_df, spark_client, spark_session
    ):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")
        spark_client.read.return_value = target_df

        # act
        file_reader.build(
            client=spark_client,
            columns=[("feature1", "new_feature1"), ("feature2", "new_feature2")],
        )
        output_df = spark_session.sql("select * from test")

        # assert
        assert check_dataframe_equality(output_df, column_target_df)
