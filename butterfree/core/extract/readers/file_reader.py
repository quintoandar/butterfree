"""FileReader entity."""

from pyspark.sql import DataFrame

from butterfree.core.clients import SparkClient
from butterfree.core.extract.readers.reader import Reader


class FileReader(Reader):
    """Responsible for get data from files.

    Attributes:
        id: unique string id for register the reader as a view on the metastore.
        path: file location.
        format: can be one of the keys: json, parquet, orc, or csv.
        format_options: additional options required by some formats. Check docs:
            https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#manually-specifying-options

    Example:
        Simple example regarding FileReader class instantiation.
    >>> file_reader = FileReader(
    ...                 id="file_reader_id",
    ...                 database="data_path",
    ...                 table="json"
    ...               )
    >>> file_reader.consume()

        This last method will use the Spark Client, as default, to read
        the desired file, loading data into a dataframe, according to
        FileReader class arguments.

    """

    def __init__(self, id: str, path: str, format: str, format_options: dict = None):
        super().__init__(id)
        if not isinstance(path, str):
            raise ValueError("path must be a string with the file location")
        if not isinstance(format, str):
            raise ValueError("format must be a string with the file type")
        self.path = path
        self.format = format
        self.options = dict(
            {"path": self.path}, **format_options if format_options else {}
        )

    def consume(self, client: SparkClient) -> DataFrame:
        """Extract data from files stored in defined path.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with all the files data.

        """
        return client.read(self.format, self.options)
