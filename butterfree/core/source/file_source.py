"""FileSource entity."""

from butterfree.core.source.source import Source


class FileSource(Source):
    """Source responsible for get data from files."""

    def __init__(self, id, client, consume_options=None, transformations=None):
        """Instantiate FileSource with the required parameters.

        :param id: unique string id for register the source as a view on the metastore
        :param client: client object from butterfree.core.client module
        :param consume_options: dict with the necessary configuration to be used in
        order for the data to be consumed.
        :param transformations: list os methods that will be applied over the dataframe
        after the raw data is extracted
        """
        super().__init__(id, client, consume_options, transformations)

    def get_data_from_file(self, path, format, format_options=None):
        """Get data from a file and returns a Spark dataframe.

        :param path: file location
        :param format: can be one of the keys: json, parquet, orc, or csv
        :param format_options: additional options required by some formats. Check docs:
        https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#manually-specifying-options
        :return: Spark dataframe
        """
        if not path or not format:
            raise ValueError("path and format parameters are required, can't be None")

        if not format_options:
            format_options = {}
        options = dict({"path": path}, **format_options)
        return self.client.load(format, options)

    def consume(self):
        """Extract data from a file source using the  defined consume_options.

        The method expects that the data will be consumed from a file source so the
        consume_options need to have set path and format keys, format_options is
        optional.
        :return: Spark dataframe
        """
        if not isinstance(self.consume_options, dict):
            raise ValueError("consume_options needs to be a dict")
        if (
            "path" not in self.consume_options.keys()
            or "format" not in self.consume_options.keys()
        ):
            raise ValueError('"path" and "format" keys needs to be in consume_options')

        path = self.consume_options["path"]
        format = self.consume_options["format"]
        format_options = (
            self.consume_options["format_options"]
            if "format_options" in self.consume_options.keys()
            else None
        )
        return self._apply_transformations(
            self.get_data_from_file(path, format, format_options)
        )
