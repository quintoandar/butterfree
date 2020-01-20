"""SparkClient entity."""

from pyspark.sql import SparkSession


class SparkClient:
    """Handle Spark session connection.

    Get query results with SQL and reads data from external systems.
    """

    def __init__(self):
        """Instantiate SparkClient with empty session."""
        self._session = None

    @property
    def conn(self):
        """Gets or creates an SparkSession.

        :return: SparkSession
        """
        if not self._session:
            self._session = SparkSession.builder.getOrCreate()
        return self._session

    def read(self, format, options):
        """Use the SparkSession.read interface to load data into a dataframe.

        :param format: string with the format to be used by the DataframeReader
        :param options: options to setup the DataframeReader, specific for each format
        :return: None
        """
        if not isinstance(format, str):
            raise ValueError("format needs to be a string with the desired read format")
        if not isinstance(options, dict):
            raise ValueError("options needs to be a dict with the setup configurations")
        return self.conn.read.format(format).options(**options).load()

    def read_table(self, database, table):
        """Use the SparkSession.read interface to load a metastore table to a dataframe.

        :param database: string with the name of the metastore schema
        :param table: string with the name of the table
        :return:
        """
        if not isinstance(database, str):
            raise ValueError(
                "database needs to be a string with the name of the metastore schema"
            )
        if not isinstance(table, str):
            raise ValueError(
                "table needs to be a string with the name of the registered table"
            )
        return self.conn.read.table("{}.{}".format(database, table))
