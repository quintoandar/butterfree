"""Holds the SourceSelector class."""

from typing import List

from pyspark.sql import DataFrame

from butterfree.core.clients import SparkClient
from butterfree.core.constants.spark_constants import DEFAULT_NUM_PARTITIONS
from butterfree.core.dataframe_service import repartition_sort_df
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

    Example:
        Simple example regarding Source class instantiation.
    >>> from butterfree.core.extract import Source
    >>> from butterfree.core.extract.readers import TableReader, FileReader
    >>> from butterfree.core.clients import SparkClient
    >>> spark_client = SparkClient()
    >>> source = Source(
    ...    readers=[
    ...        TableReader(
    ...            id="table_reader_id",
    ...            database="table_reader_db",
    ...            table="table_reader_table",
    ...        ),
    ...        FileReader(id="file_reader_id", path="data_sample_path", format="json"),
    ...    ],
    ...    query=f"select a.*, b.feature2 "
    ...    f"from table_reader_id a "
    ...    f"inner join file_reader_id b on a.id = b.id ",
    ...)
    >>> df = source.construct(spark_client)

        This last method will use the Spark Client, as default, to create
        temporary views regarding each reader and, after, will run the
        desired query and return a dataframe.

    """

    def __init__(self, readers: List[Reader], query: str,) -> None:
        self.readers = readers
        self.query = query

    def construct(
        self,
        client: SparkClient,
        partition_by: List[str] = None,
        num_partitions: int = None,
        num_processors: int = None,
    ) -> DataFrame:
        """Construct an entry point dataframe for a feature set.

        This method will assemble multiple readers, by building each one and
        querying them using a Spark SQL.

        After that, there's the caching of the dataframe, however since cache()
        in Spark is lazy, an action is triggered in order to force persistence.

        Args:
            client: client responsible for connecting to Spark session.
            partition_by: columns to be partitioned.
            num_partitions: number of partitions.
            num_processors: number of cluster processors.

        Returns:
            DataFrame with the query result against all readers.

        """
        for reader in self.readers:
            reader.build(client)  # create temporary views for each reader

        dataframe = client.sql(self.query)

        if partition_by:
            num_partitions = (
                num_processors * 40
                if num_processors
                else num_partitions or DEFAULT_NUM_PARTITIONS
            )
            dataframe = repartition_sort_df(dataframe, num_partitions, partition_by)

        if not dataframe.isStreaming:
            dataframe.cache().count()

        return dataframe
