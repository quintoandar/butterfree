"""Spark table schema compatibility Hook definition."""

from pyspark.sql import DataFrame

from butterfree.clients import SparkClient
from butterfree.hooks.hook import Hook


class SparkTableSchemaCompatibilityHook(Hook):
    """Hook to verify the schema compatibility with a Spark's table.

    Verifies if all columns presented on the dataframe exists and are the same
    type on the target Spark's table.

    Attributes:
        spark_client: client to connect to Spark's metastore.
        table: table name.
        database: database name.
    """

    def __init__(self, spark_client: SparkClient, table: str, database: str = None):
        self.spark_client = spark_client
        self.table_expression = (f"`{database}`." if database else "") + f"`{table}`"

    def run(self, dataframe: DataFrame) -> DataFrame:
        """Check the schema compatibility from a given Dataframe.

        This method does not change anything on the Dataframe.

        Args:
            dataframe: dataframe to verify schema compatibility.

        Returns:
            unchanged dataframe.

        Raises:
            ValueError if the schemas are incompatible.
        """
        table_schema = self.spark_client.conn.table(self.table_expression).schema
        if not all([column in table_schema for column in dataframe.schema]):
            raise ValueError(
                "The dataframe has a schema incompatible with the defined table.\n"
                f"Dataframe schema = {dataframe.schema}"
                f"Target table schema = {table_schema}"
            )
        return dataframe
