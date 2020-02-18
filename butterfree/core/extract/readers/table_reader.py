"""TableSource entity."""

from pyspark.sql import DataFrame

from butterfree.core.clients import SparkClient
from butterfree.core.extract.readers.reader import Reader


class TableReader(Reader):
    """Responsible for get data from tables registered in the metastore.

    Attributes:
        id: unique string id for register the reader as a view on the metastore.
        database: name of the metastore database/schema.
        table: name of the table.

    """

    def __init__(self, id: str, database: str, table: str):
        super().__init__(id)
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

    def consume(self, client: SparkClient) -> DataFrame:
        """Extract data from a table in Spark metastore.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with all the data from the table.

        """
        return client.read_table(self.database, self.table)
