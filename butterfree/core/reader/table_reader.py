"""TableSource entity."""

from butterfree.core.client import SparkClient
from butterfree.core.reader.reader import Reader


class TableReader(Reader):
    """Reader responsible for get data from tables registered in the metastore."""

    def __init__(self, id, database, table):
        """Instantiate TableReader with the required parameters.

        :param id: unique string id for register the reader as a view on the metastore
        :param spark_client: spark_client object client module
        :param database: string with the name of the metastore schema
        :param table: string with the name of the table
        """
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

    def consume(self, client: SparkClient):
        """Extract data from a table reader.

        :return: Spark dataframe
        """
        return client.read_table(self.database, self.table)
