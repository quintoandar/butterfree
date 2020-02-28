"""SparkClient entity."""

from typing import List

from pyspark.sql import DataFrame, SparkSession

from butterfree.core.configs import environment


class SparkClient:
    """Handle Spark session connection.

    Get query results with SQL, reads and writes data on external systems.

    """

    def __init__(self):
        self._session = None

    @property
    def conn(self) -> SparkSession:
        """Gets or creates an SparkSession.

        Returns:
            Spark session

        """
        if not self._session:
            self._session = SparkSession.builder.getOrCreate()
            if environment.get_variable("ENVIRONMENT", "dev"):
                hadoop_conf = self._session.sparkContext._jsc.hadoopConfiguration()
                hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                hadoop_conf.set("fs.s3a.endpoint", "http://s3.amazonaws.com:4572")
                hadoop_conf.set("fs.s3a.path.style.access", "true")
                hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
                hadoop_conf.set("fs.s3a.access.key", "mock")
                hadoop_conf.set("fs.s3a.secret.key", "mock")
        return self._session

    def read(self, format: str, options: dict, stream: bool = False) -> DataFrame:
        """Use the SparkSession.read interface to load data into a dataframe.

        Check docs for more information:
            https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#generic-loadsave-functions

        Args:
            format: string with the format to be used by the DataframeReader.
            options: options to setup the DataframeReader.
            stream:  flag to indicate if data must be read in stream mode.

        Returns:
            Dataframe

        """
        if not isinstance(format, str):
            raise ValueError("format needs to be a string with the desired read format")
        if not isinstance(options, dict):
            raise ValueError("options needs to be a dict with the setup configurations")
        df_reader = self.conn.readStream if stream else self.conn.read
        return df_reader.format(format).options(**options).load()

    def read_table(self, database: str, table: str) -> DataFrame:
        """Use the SparkSession.read interface to read a metastore table.

        Args:
            database: name of the metastore databse/schema
            table: name of the table in metastore

        Returns:
            Dataframe

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
        """Run a query using Spark SQL.

        Args:
            query: Spark SQL query.

        Returns:
            Dataframe

        """
        return self.conn.sql(query)

    @staticmethod
    def write_dataframe(dataframe: DataFrame, format_: str, mode: str, **options):
        """Receive a spark DataFrame and write it.

        Args:
            dataframe: dataframe containing data from a feature set.
            format_: format used to save the dataframe.
            mode: writting modem can be "error", "append", "overwrite" or
                "ignore". For more informations:
                [here](https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#save-modes).
            **options: all other options that can be used in a DataFrameWriter.

        """
        if not isinstance(format_, str):
            raise ValueError("format needs to be a string")
        if not isinstance(mode, str):
            raise ValueError("mode needs to be a string")
        dataframe.write.save(format=format_, mode=mode, **options)

    @staticmethod
    def write_table(
        dataframe: DataFrame,
        database: str,
        table_name: str,
        path: str,
        format_: str = None,
        mode: str = None,
        partition_by: List[str] = None,
        **options,
    ):
        """Receive a spark DataFrame and write it as a table in metastore.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            database: specified database name.
            table_name: specified table name.
            path: string with the local to save the table.
            format_: string with the format used to save.
            mode: writing mode, it can be: "error", "append", "overwrite" or
                "ignore". More information:
                [here](https://spark.apache.org/docs/2.3.0/sql-programming-guide.html#save-modes).
            partition_by: names of partitioning columns.
            options: all other options that can be used in a DataFrameWriter.

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
