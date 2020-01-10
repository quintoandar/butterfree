from pyspark.sql.functions import col, lit

from butterfree.core.consumers.consumer import Consumer


class SparkConsumer(Consumer):
    """
    Gets data from a Databrick Metastore through Spark and returns it as a Spark
    DataFrame.
    :param spark_client: A client to handle the Spark connection
    :type spark_client: SparkClient
    """

    def __init__(self, spark_client):
        self.spark_client = spark_client

    def consume(self, options):
        """
        Uses the proper function to fecth data.
        :param options: Consumer options to read the data
        :return: A DataFrame with the desired data
        """
        if all(key in options.keys() for key in ("query", "db")):
            df = self.get_data_from_query(options["query"], options["db"])
        elif all(key in options.keys() for key in ("db", "table_name")):
            df = self.get_table_schema(options["db"], options["table_name"])
        else:
            raise ValueError()

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
        query = "select * from {db}.{table}".format(db=db, table=table_name)
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
