"""FileReader entity."""
from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from butterfree.clients import SparkClient
from butterfree.extract.readers.reader import Reader


class FileReader(Reader):
    """Responsible for get data from files.

    Attributes:
        id: unique string id for register the reader as a view on the metastore.
        path: file location.
        format: can be one of the keys: json, parquet, orc, or csv.
        schema: an optional pyspark.sql.types.StructType for the input schema.
        format_options: additional options required by some formats. Check docs:
            https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#manually-specifying-options

    Example:
        Simple example regarding FileReader class instantiation.

    >>> from butterfree.extract.readers import FileReader
    >>> from butterfree.clients import SparkClient
    >>> from butterfree.extract.pre_processing import filter
    >>> spark_client = SparkClient()
    >>> file_reader = FileReader(
    ...                 id="file_reader_id",
    ...                 path="data_path",
    ...                 format="json"
    ...               )
    >>> df = file_reader.consume(spark_client)

        However, we can define the schema and format_options,
        like header, and provide them to FileReader.

    >>> spark_client = SparkClient()
    >>> schema_csv = StructType([
    ...            StructField("column_a", LongType()),
    ...            StructField("column_b", DoubleType()),
    ...            StructField("coumn_c", StringType())
    ...          ])
    >>> file_reader = FileReader(
    ...                 id="file_reader_id",
    ...                 path="data_path",
    ...                 format="csv",
    ...                 schema=schema_csv,
    ...                 format_options={
    ...                    "header": True
    ...                 }
    ...               )
    >>> df = file_reader.consume(spark_client)


        This last method will use the Spark Client, as default, to read
        the desired file, loading data into a dataframe, according to
        FileReader class arguments.

        It's also possible to define simple transformations within the
        reader's scope:

    >>> file_reader.with_(filter, condition="year = 2019").build(spark_client)

        In this case, however, a temp view will be created, cointaining
        the transformed data.

    """

    __name__ = "File Reader"

    def __init__(
        self,
        id: str,
        path: str,
        format: str,
        schema: StructType = None,
        format_options: Dict[Any, Any] = None,
        stream: bool = False,
    ):
        super().__init__(id)
        if not isinstance(path, str):
            raise ValueError("path must be a string with the file location")
        if not isinstance(format, str):
            raise ValueError("format must be a string with the file type")
        self.path = path
        self.format = format
        self.schema = schema
        self.options = dict(format_options if format_options else {})
        self.stream = stream

    def consume(self, client: SparkClient) -> DataFrame:
        """Extract data from files stored in defined path.

        Try to auto-infer schema if in stream mode and not manually defining a
        schema.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with all the files data.

        """
        schema = (
            client.read(format=self.format, path=self.path, **self.options).schema
            if (self.stream and not self.schema)
            else self.schema
        )

        return client.read(
            format=self.format,
            schema=schema,
            stream=self.stream,
            path=self.path,
            **self.options,
        )
