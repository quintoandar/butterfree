import pytest
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from butterfree.clients import SparkClient
from butterfree.extract.readers import FileReader


class TestFileReader:
    @pytest.mark.parametrize(
        "path, format", [(None, "parquet"), ("path/to/file.json", 123), (123, None,)],
    )
    def test_init_invalid_params(self, path, format):
        # act and assert
        with pytest.raises(ValueError):
            FileReader("id", path, format)

    @pytest.mark.parametrize(
        "path, format, schema, format_options",
        [
            ("path/to/file.parquet", "parquet", None, None),
            ("path/to/file.json", "json", None, None),
            (
                "path/to/file.json",
                "csv",
                None,
                {"sep": ",", "header": False, "inferSchema": True},
            ),
        ],
    )
    def test_consume(
        self, path, format, schema, format_options, spark_client, target_df
    ):
        # arrange
        spark_client.read.return_value = target_df
        file_reader = FileReader("test", path, format, schema, format_options)

        # act
        output_df = file_reader.consume(spark_client)
        options = dict({"path": path}, **format_options if format_options else {})

        # assert
        spark_client.read.assert_called_once_with(
            format=format, options=options, schema=schema, stream=False
        )
        assert target_df.collect() == output_df.collect()

    def test_consume_with_stream_without_schema(self, spark_client, target_df):
        # arrange
        path = "path/to/file.json"
        format = "json"
        schema = None
        format_options = None
        stream = True
        options = dict({"path": path})

        spark_client.read.return_value = target_df
        file_reader = FileReader(
            "test", path, format, schema, format_options, stream=stream
        )

        # act
        output_df = file_reader.consume(spark_client)

        # assert

        # assert call for schema infer
        spark_client.read.assert_any_call(format=format, options=options)
        # assert call for stream read
        # stream
        spark_client.read.assert_called_with(
            format=format, options=options, schema=output_df.schema, stream=stream
        )
        assert target_df.collect() == output_df.collect()

    def test_json_file_with_schema(self):
        # given
        spark_client = SparkClient()
        schema_json = StructType(
            [
                StructField("A", StringType()),
                StructField("B", DoubleType()),
                StructField("C", StringType()),
            ]
        )

        file = "tests/unit/butterfree/extract/readers/file-reader-test.json"

        # when
        file_reader = FileReader(id="id", path=file, format="json", schema=schema_json)
        df = file_reader.consume(spark_client)

        # assert
        assert schema_json == df.schema

    def test_csv_file_with_schema_and_header(self):
        # given
        spark_client = SparkClient()
        schema_csv = StructType(
            [
                StructField("A", LongType()),
                StructField("B", DoubleType()),
                StructField("C", StringType()),
            ]
        )

        file = "tests/unit/butterfree/extract/readers/file-reader-test.csv"

        # when
        file_reader = FileReader(
            id="id",
            path=file,
            format="csv",
            schema=schema_csv,
            format_options={"header": True},
        )
        df = file_reader.consume(spark_client)

        # assert
        assert schema_csv == df.schema
        assert df.columns == ["A", "B", "C"]
        for value in range(3):
            assert df.first()[value] != ["A", "B", "C"][value]
