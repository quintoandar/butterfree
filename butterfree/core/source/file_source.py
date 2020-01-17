"""FileSource entity."""

from butterfree.core.source.source import Source


class FileSource(Source):
    """Source responsible for get data from files."""

    def __init__(self, id, spark_client, path, format, format_options=None):
        """Instantiate FileSource with the required parameters.

        :param id: unique string id for register the source as a view on the metastore
        :param spark_client: spark_client object client module
        :param path: file location
        :param format: can be one of the keys: json, parquet, orc, or csv
        :param format_options: additional options required by some formats. Check docs:
        https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#manually-specifying-options
        """
        super().__init__(id, spark_client)
        if not isinstance(path, str):
            raise ValueError("path must be a string with the file location")
        if not isinstance(format, str):
            raise ValueError("format must be a string with the file type")
        if not format_options:
            format_options = {}
        self.path = path
        self.format = format
        self.format_options = format_options

    def consume(self):
        """Extract data from a file source.

        :return: Spark dataframe
        """
        options = dict({"path": self.path}, **self.format_options)
        return self.client.load(self.format, options)
