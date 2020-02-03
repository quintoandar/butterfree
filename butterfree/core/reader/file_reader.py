"""FileReader entity."""

from butterfree.core.client import SparkClient
from butterfree.core.reader.reader import Reader


class FileReader(Reader):
    """Reader responsible for get data from files."""

    def __init__(self, id, path, format, format_options=None):
        """Instantiate FileReader with the required parameters.

        :param id: unique string id for register the reader as a view on the metastore
        :param spark_client: spark_client object client module
        :param path: file location
        :param format: can be one of the keys: json, parquet, orc, or csv
        :param format_options: additional options required by some formats. Check docs:
        https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#manually-specifying-options
        """
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

    def consume(self, client: SparkClient):
        """Extract data from a file reader.

        :return: Spark dataframe
        """
        return client.read(self.format, self.options)
