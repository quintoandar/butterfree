"""TableSource entity."""

from pyspark.sql import DataFrame

from butterfree.clients import SparkClient
from butterfree.extract.readers.reader import Reader


class TableReader(Reader):
    """Responsible for get data from tables registered in the metastore.

    Attributes:
        id: unique string id for register the reader as a view on the metastore.
        database: name of the metastore database/schema.
        table: name of the table.

    Example:
        Simple example regarding TableReader class instantiation.

    >>> from butterfree.extract.readers import TableReader
    >>> from butterfree.clients import SparkClient
    >>> from butterfree.extract.pre_processing import filter
    >>> spark_client = SparkClient()
    >>> table_reader = TableReader(
    ...                     id="table_reader_id",
    ...                     database="table_reader_db",
    ...                     table="table_reader_table"
    ...                )
    >>> df = table_reader.consume(spark_client)

        This last method will use the Spark Client, as default, to read
        the desired table, loading data into a dataframe, according to
        TableReader class arguments.

        It's also possible to define simple transformations within the
        reader's scope:

    >>> table_reader.with_(filter, condition="year = 2019").build(spark_client)

        In this case, however, a temp view will be created, cointaining
        the transformed data.

    """

    __name__ = "Table Reader"

    def __init__(self, id: str, table: str, database: str = None):
        super().__init__(id)
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
        return client.read_table(self.table, self.database)
