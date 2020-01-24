"""SparkClient entity."""

from pyspark.sql import DataFrame, SparkSession


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

    def read(self, format, options, stream=False):
        """Use the SparkSession.read interface to load data into a dataframe.

        :param format: string with the format to be used by the DataframeReader
        :param options: options to setup the DataframeReader, specific for each format
        :return: Spark dataframe
        """
        if not isinstance(format, str):
            raise ValueError("format needs to be a string with the desired read format")
        if not isinstance(options, dict):
            raise ValueError("options needs to be a dict with the setup configurations")
        df_reader = self.conn.readStream if stream else self.conn.read
        return df_reader.format(format).options(**options).load()

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

    def sql(self, query: str) -> DataFrame:
        """Run a query using spark.

        :param query: Spark SQL query.
        :return dataframe: Spark DataFrame with the query result.
        """
        return self.conn.sql(query)

    @staticmethod
    def write_dataframe(dataframe, format_, mode, **options):
        """Receive a spark DataFrame and write it.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            format_: string with the format used to save.
            mode: specified function mode when data already exists,
                mode can be "error", "append", "overwrite" and "ignore".
                For more informations:
                [here](https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#save-modes).
            options: all other string options.
        """
        if not isinstance(format_, str):
            raise ValueError("format needs to be a string")
        if not isinstance(mode, str):
            raise ValueError("mode needs to be a string")

        dataframe.write.save(format=format_, mode=mode, **options)

    @staticmethod
    def write_table(
        dataframe,
        database,
        table_name,
        path,
        format_=None,
        mode=None,
        partition_by=None,
        **options,
    ):
        """Receive a spark DataFrame and write it as a table.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            database: specified database name.
            table_name: specified table name.
            path: string with the local to save the table.
            format_: string with the format used to save.
            mode: specified function mode when data already exists,
                mode can be "error", "append", "overwrite" and "ignore".
                For more informations:
                [here](https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#save-modes).
            partition_by: names of partitioning columns.
            options: all other string options.
        """
        if not isinstance(database, str):
            raise ValueError("database needs to be a string")
        if not isinstance(table_name, str):
            raise ValueError("table_name needs to be a string")
        if not isinstance(path, str):
            raise ValueError("path needs to be a string of the local to save")

        name = "{}.{}".format(database, table_name)

        dataframe.write.saveAsTable(
            mode=mode,
            format=format_,
            partitionBy=partition_by,
            name=name,
            path=path,
            **options,
        )
