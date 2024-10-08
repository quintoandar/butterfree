"""Holds the SourceSelector class."""

from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel

from butterfree.clients import SparkClient
from butterfree.extract.readers.reader import Reader
from butterfree.hooks import HookableComponent


class Source(HookableComponent):
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

    >>> from butterfree.extract import Source
    >>> from butterfree.extract.readers import TableReader, FileReader
    >>> from butterfree.clients import SparkClient
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

        The `eager_evaluation` param forces Spark to apply the currently
        mapped changes to the DataFrame. When this parameter is set to
        False, Spark follows its standard behaviour of lazy evaluation.
        Lazy evaluation can improve Spark's performance as it allows
        Spark to build the best version of the execution plan.

    """

    def __init__(
        self,
        readers: List[Reader],
        query: str,
        eager_evaluation: bool = True,
    ) -> None:
        super().__init__()
        self.enable_pre_hooks = False
        self.readers = readers
        self.query = query
        self.eager_evaluation = eager_evaluation

    def construct(
        self,
        client: SparkClient,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> DataFrame:
        """Construct an entry point dataframe for a feature set.

        This method will assemble multiple readers, by building each one and
        querying them using a Spark SQL. It's important to highlight that in
        order to filter a dataframe regarding date boundaries, it's important
        to define a IncrementalStrategy, otherwise your data will not be filtered.
        Besides, both start and end dates parameters are optional.

        After that, there's the caching of the dataframe, however since cache()
        in Spark is lazy, an action is triggered in order to force persistence.

        Args:
            client: client responsible for connecting to Spark session.
            start_date: user defined start date for filtering.
            end_date: user defined end date for filtering.

        Returns:
            DataFrame with the query result against all readers.

        """
        # Step 1: Build temporary views for each reader
        for reader in self.readers:
            reader.build(client=client, start_date=start_date, end_date=end_date)

        # Step 2: Execute SQL query on the combined readers
        dataframe = client.sql(self.query)

        # Step 3: Cache the dataframe if necessary, using memory and disk storage
        if not dataframe.isStreaming and self.eager_evaluation:
            # Persist to ensure the DataFrame is stored in mem and disk (if necessary)
            dataframe.persist(StorageLevel.MEMORY_AND_DISK)
            # Trigger the cache/persist operation by performing an action
            dataframe.count()

        # Step 4: Run post-processing hooks on the dataframe
        post_hook_df = self.run_post_hooks(dataframe)

        return post_hook_df
