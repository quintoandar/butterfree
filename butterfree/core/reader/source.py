"""Holds the SourceSelector class."""

from typing import List

from pyspark.sql import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.reader import Reader


class Source:
    """Constructor for a single reader.

    The single reader will be used as an entrypoint for the Feature set transformations.
    A feature set should be built from a single dataframe, which is a composition of
    multiple sources.

    TODO refactor query into multiple query components
    TODO make it harder to do query injection

    Attributes:
        spark_client: client used to run a query.
        readers: list of readers from where the source will get data.
        query: Spark SQL query to run against the sources.

    """

    def __init__(
        self, spark_client: SparkClient, readers: List[Reader], query: str
    ) -> None:
        self.readers = readers
        self.query = query
        self.client = spark_client

    def construct(self) -> DataFrame:
        """Construct an entry point dataframe for a feature set.

        This method will assemble multiple readers, by building each one and
        querying them using a Spark SQL.

        After that, there's the caching of the dataframe, however since cache() in
        Spark is lazy, an action is triggered in order to force persistence.

        Returns:
            DataFrame with the query result against all readers.

        """
        for reader in self.readers:
            reader.build()  # create temporary views for each reader

        dataframe = self.client.sql(self.query)
        dataframe.cache().count()

        return dataframe
