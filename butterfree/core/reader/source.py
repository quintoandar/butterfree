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
    """

    def __init__(self, readers: List[Reader], query: str) -> None:
        """Initialize a SourceSelector.

        :param spark_client: client used to run a query.
        :param sources: list of sources from where the selector will get data.
        :param query: Spark SQL query to run against the sources.
        """
        self.readers = readers
        self.query = query

    def construct(self, client: SparkClient) -> DataFrame:
        """Construct an entry point dataframe for a feature set.

        This method will assemble multiple sources, by building each one and querying
        data using a Spark SQL query.

        After that, there's the caching of the dataframe, however since cache() in
        Spark is lazy, an action is triggered in order to force persistence.

        :return: Spark DataFrame with the query result against all sources.
        """
        for reader in self.readers:
            reader.build(client)  # create temporary views for each reader

        dataframe = client.sql(self.query)
        dataframe.cache().count()

        return dataframe
