import pytest
from pyspark.sql.functions import expr

from butterfree.dataframe_service import IncrementalStrategy
from butterfree.extract.readers import FileReader
from butterfree.testing.dataframe import assert_dataframe_equality


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
        input_df = spark_session.read.json(spark_context.parallelize(input_data, 1))
        target_df = spark_session.read.json(
            spark_context.parallelize(transformed_data, 1)
        )

        # act
        result_df = file_reader._apply_transformations(input_df)

        # assert
        assert target_df.collect() == result_df.collect()

    def test_build(self, target_df, spark_client, spark_session):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")
        spark_client.read.return_value = target_df

        # act
        file_reader.build(spark_client)
        result_df = spark_session.sql("select * from test")

        # assert
        assert target_df.collect() == result_df.collect()

    def test_build_with_columns(
        self, target_df, column_target_df, spark_client, spark_session
    ):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")
        spark_client.read.return_value = target_df

        # act
        file_reader.build(
            client=spark_client,
            columns=[("col1", "new_col1"), ("col2", "new_col2")],
        )
        result_df = spark_session.sql("select * from test")

        # assert
        assert column_target_df.collect() == result_df.collect()

    def test_build_with_incremental_strategy(
        self, incremental_source_df, spark_client, spark_session
    ):
        # arrange
        readers = [
            # directly from column
            FileReader(
                id="test_1", path="path/to/file", format="format"
            ).with_incremental_strategy(
                incremental_strategy=IncrementalStrategy(column="date")
            ),
            # from milliseconds
            FileReader(
                id="test_2", path="path/to/file", format="format"
            ).with_incremental_strategy(
                incremental_strategy=IncrementalStrategy().from_milliseconds(
                    column_name="milliseconds"
                )
            ),
            # from str
            FileReader(
                id="test_3", path="path/to/file", format="format"
            ).with_incremental_strategy(
                incremental_strategy=IncrementalStrategy().from_string(
                    column_name="date_str", mask="dd/MM/yyyy"
                )
            ),
            # from year, month, day partitions
            FileReader(
                id="test_4", path="path/to/file", format="format"
            ).with_incremental_strategy(
                incremental_strategy=(
                    IncrementalStrategy().from_year_month_day_partitions()
                )
            ),
        ]

        spark_client.read.return_value = incremental_source_df
        target_df = incremental_source_df.where(
            "date >= date('2020-07-29') and date <= date('2020-07-31')"
        )

        # act
        for reader in readers:
            reader.build(
                client=spark_client, start_date="2020-07-29", end_date="2020-07-31"
            )

        output_dfs = [
            spark_session.table(f"test_{i + 1}") for i, _ in enumerate(readers)
        ]

        # assert
        for output_df in output_dfs:
            assert_dataframe_equality(output_df=output_df, target_df=target_df)
