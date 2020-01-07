from pyspark.sql import SparkSession


class SparkClient:
    """
    Run commands, return query results and reads data from external systems with Spark.
    """

    def __init__(self):
        self._session = None

    @property
    def conn(self):
        """
        Gets or creates an SparkSession
        :return: SparkSession
        """
        if not self._session:
            self._session = SparkSession.builder.getOrCreate()
        return self._session

    def get_records(self, query, parameters=None):
        df = self.conn.sql(query)

        return df

    def run(self, command, autocommit=False, parameters=None):
        self.conn.sql(command)

    def get_data_from_external_source(self, format, options):
        """
        Gets data from an external source with spark.
        :param format: The format of the connection (e.g. jdbc, mongo)
        :param options: Spark options to read the data (e.g. url, user, pwd)
        :return: A Spark DataFrame
        """
        stmt = self.conn.read.format(format)
        for op, val in options.items():
            stmt = stmt.option(op, val)
        df = stmt.load()

        return df

    def create_dataframe(
        self, data, schema=None, sampling_ratio=None, verify_schema=True
    ):
        """
        Creates a DataFrame from an RDD, a list or a pandas.DataFrame. This method is
        a wrapper to the spark.createDataFrame method. In case of doubt, check the
        docs of the wrapped method.
        :param data: an RDD of any kind of SQL data representation(e.g. row, tuple,
        int, boolean, etc.), or list, or pandas.DataFrame.
        :param schema:  a pyspark.sql.types.DataType or a datatype string or a list
        of column names, default is None. The data type string format equals to
        pyspark.sql.types.DataType.simpleString, except that top level struct type
        can omit the struct<> and atomic types use typeName() as their format,
        e.g. use byte instead of tinyint for
        :param sampling_ratio: the sample ratio of rows used for inferring
        :param verify_schema: verify data types of every row against schema
        :return: A Spark DataFrame
        """
        df = self.conn.createDataFrame(data, schema, sampling_ratio, verify_schema)

        return df
