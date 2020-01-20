"""TableSource entity."""

from butterfree.core.source.source import Source


class TableSource(Source):
    """Source responsible for get data from tables registered in the metastore."""

    def __init__(self, id, spark_client, database, table):
        """Instantiate TableSource with the required parameters.

        :param id: unique string id for register the source as a view on the metastore
        :param spark_client: spark_client object client module
        :param database: string with the name of the metastore schema
        :param table: string with the name of the table
        """
        super().__init__(id, spark_client)
        if not isinstance(database, str):
            raise ValueError("path must be a string with the file location")
        if not isinstance(table, str):
            raise ValueError("format must be a string with the file type")
        self.database = database
        self.table = table

    def consume(self):
        """Extract data from a table source.

        :return: Spark dataframe
        """
        return self.client.read_table(self.database, self.table)
