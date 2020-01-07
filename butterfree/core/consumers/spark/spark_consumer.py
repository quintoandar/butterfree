from pyspark.sql.functions import col, lit


class SparkConsumer:
    """
    Gets data from a Databrick Metastore through Spark and returns it as a Spark
    DataFrame.
    :param conn_config: A dict with the config values of the connection. It must
    contain the key `db`.
    :type conn_config: dict
    :param spark_client: A client to handle the Spark connection
    :type spark_client: SparkClient
    """

    def __init__(self, spark_client):
        self.spark_client = spark_client

    def get_table_names_and_sizes(self, db):
        """
        Gets the table names and sizes in the given database.
        :return: A Spark DataFrame with cols: table_name and size
        """
        query = "show tables in {db}".format(db=db)
        df = (
            self.spark_client.get_records(query)
            .select(col("tableName").alias("table_name"))
            .withColumn("size", lit(0))
        )

        return df

    def get_table_schema(self, db, table_name):
        """
        Gets the schema of a table in the given database.
        :param table_name: Name of a table
        :return: A Spark DataFrame with the table schema
        """
        query = "describe {db}.{table}".format(db=db, table=table_name)
        df = (
            self.spark_client.get_records(query)
            .select("col_name", col("data_type").alias("col_type"))
            .filter(col("col_name").rlike(r"^\w"))
            .distinct()
        )

        return df

    def get_data_from_table(self, db, table_name):
        query = "show tables in {db}.{table}".format(db=db, table=table_name)
        df = self.spark_client.get_records(query)

        return df

    def get_data_from_query(self, query, db, table_name=None):
        """
        Gets the results of a query.
        :param query: Query content
        :param table_name: Name of the table relevant to the query
        :return: A Spark DataFrame with the query results
        """
        self.spark_client.run("USE {}".format(db))
        df = self.spark_client.get_records(query)

        return df
