"""Holds the SourceSelector class."""

from typing import List

from pyspark.sql import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.source import Source


class SourceSelector:
    """Constructor for a single source.

    The single source will be used as an entrypoint for the Feature set transformations.
    A feature set should be built from a single dataframe, which is a composition of
    multiple sources.

    TODO refactor query into multiple query components
    TODO make it harder to do query injection
    """

    def __init__(
        self, spark_client: SparkClient, sources: List[Source], query: str
    ) -> None:
        """Initialize a SourceSelector.

        :param spark_client: client used to run a query.
        :param sources: list of sources from where the selector will get data.
        :param query: Spark SQL query to run against the sources.
        """
        self.sources = sources
        self.query = query
        self.client = spark_client

    def construct(self) -> DataFrame:
        """Construct an entry point dataframe for a feature set.

        This method will assemble multiple sources, by building each one and querying
        data using a Spark SQL query.

        :return: Spark DataFrame with the query result against all sources.
        """
        for source in self.sources:
            source.build()  # create temporary views for each source

        return self.client.sql(self.query)
