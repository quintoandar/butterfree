"""TableSource entity."""

from pyspark.sql import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.reader.reader import Reader


class TableReader(Reader):
    """Responsible for get data from tables registered in the metastore.

    Attributes:
        id: unique string id for register the reader as a view on the metastore
        spark_client: client for connection to the metastore
        database: name of the metastore database/schema
        table: name of the table

    """

    def __init__(self, id: str, spark_client: SparkClient, database: str, table: str):
        super().__init__(id, spark_client)
        if not isinstance(database, str):
            raise ValueError(
                "database needs to be a string with the name of the metastore schema"
            )
        if not isinstance(table, str):
            raise ValueError(
                "table needs to be a string with the name of the registered table"
            )
        self.database = database
        self.table = table

    def consume(self) -> DataFrame:
        """Extract data from the table."""
        return self.client.read_table(self.database, self.table)
