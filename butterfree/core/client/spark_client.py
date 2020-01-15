from pyspark.sql import SparkSession, DataFrameReader


class SparkClient:
    """
    Handle Spark session connection, get query results and reads data from external
    systems.
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

    @staticmethod
    def load(spark_df_reader, **kwargs):
        """
        Call the load method of a Spark dataframe reader. This action will write the
        dataframe to the desired location.
        :param spark_df_reader: pyspark.sql.DataFrameReader object
        :param kwargs: kwargs to be used in the load method
        :return: None
        """
        if not isinstance(spark_df_reader, DataFrameReader):
            raise ValueError(
                "spark_df_reader needs to be a instance of pyspark.sql.DataFrameReader"
            )
        return spark_df_reader.load(**kwargs)

    def get_records(self, query):
        """
        Get records from a query executed over the Spark metastore.
        :param query: string with the query to be executed
        :return: Spark dataframe
        """
        return self.conn.sql(query)
