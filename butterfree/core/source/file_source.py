from butterfree.core.source import Source


class FileSource(Source):
    """
    Source responsible for get data from files
    """

    def __init__(self, id, client, consume_options=None, transformations=None):
        super().__init__(id, client, consume_options, transformations)

    def get_data_from_file(self, path, format, format_options=None):
        """
        Get data from a file and returns a Spark dataframe.
        :param path: file location
        :param format: can be one of the keys: json, parquet, orc, or csv
        :param format_options: additional options required by some formats
        :return: Spark dataframe
        """
        if not path or not format:
            raise ValueError("path and format parameters are required, can't be None")

        if not format_options:
            format_options = {}

        spark_df_reader = self.client.conn.read.format(format)
        kwargs = dict({"path": path}, **format_options)
        return self.client.load(spark_df_reader, **kwargs)

    def consume(self):
        if not isinstance(self.consume_options, dict):
            raise ValueError("onsume_options needs to be a dict")
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
