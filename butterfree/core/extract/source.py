"""Holds the SourceSelector class."""

from typing import List

from pyspark.sql import DataFrame

from butterfree.core.clients import SparkClient
from butterfree.core.extract.readers.reader import Reader


class Source:
    """The definition of the the entry point data for the ETL pipeline.

    A FeatureSet (the next step in the pipeline) expects a single dataframe as
    input. This dataframe is built from a data composition of one or more
    readers defined in the Source. There is only one Source for pipeline.

    TODO refactor query into multiple query components
    TODO make it harder to do query injection

    Attributes:
        readers: list of readers from where the source will get data.
        query: Spark SQL query to run against the readers.

    """

    def __init__(self, readers: List[Reader], query: str) -> None:
        self.readers = readers
        self.query = query

    def construct(self, client: SparkClient) -> DataFrame:
        """Construct an entry point dataframe for a feature set.

        This method will assemble multiple readers, by building each one and
        querying them using a Spark SQL.

        After that, there's the caching of the dataframe, however since cache()
        in Spark is lazy, an action is triggered in order to force persistence.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            DataFrame with the query result against all readers.

        """
        for reader in self.readers:
            reader.build(client)  # create temporary views for each reader

        dataframe = client.sql(self.query)
        dataframe.cache().count()

        return dataframe
