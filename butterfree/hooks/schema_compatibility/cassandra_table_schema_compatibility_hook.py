"""Cassandra table schema compatibility Hook definition."""

from pyspark.sql import DataFrame

from butterfree.clients import CassandraClient
from butterfree.constants import DataType
from butterfree.hooks.hook import Hook


class CassandraTableSchemaCompatibilityHook(Hook):
    """Hook to verify the schema compatibility with a Cassandra's table.

    Verifies if all columns presented on the dataframe exists and are the same
    type on the target Cassandra's table.

    Attributes:
        cassandra_client: client to connect to Cassandra DB.
        table: table name.
    """

    def __init__(self, cassandra_client: CassandraClient, table: str):
        self.cassandra_client = cassandra_client
        self.table = table

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
        table_schema = self.cassandra_client.get_schema(self.table)
        type_cassandra = [
            type.cassandra
            for field_id in range(len(dataframe.schema.fieldNames()))
            for type in DataType
            if dataframe.schema.fields.__getitem__(field_id).dataType == type.spark
        ]
        schema = [
            {"column_name": f"{column}", "type": f"{type}"}
            for column, type in zip(dataframe.columns, type_cassandra)
        ]

        if not all([column in table_schema for column in schema]):
            raise ValueError(
                "There's a schema incompatibility "
                "between the defined dataframe and the Cassandra table.\n"
                f"Dataframe schema = {schema}"
                f"Target table schema = {table_schema}"
            )
        return dataframe
