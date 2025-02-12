# butterfree/load/writers/delta_feature_store_writer.py
from dataclasses import dataclass
from typing import List, Optional

from butterfree.load.writers import DeltaWriter
from butterfree.load.writers.writer import Writer


@dataclass
class DeltaConfig:
    """Delta merge configuration parameters.

    Defines parameters for controlling Delta merge operations, including merge keys,
    deduplication settings, and conditional merge behaviors.

    Attributes:
        database: Target database name for the Delta table.
        table: Target table name for the Delta table.
        merge_on: List of columns to use as merge keys. Values in these columns should
            uniquely identify rows for merging.
        deduplicate: Whether to deduplicate source data before merging based on feature
            set keys. Defaults to False.
        when_not_matched_insert: Control when new records
            should be inserted. When provided, records will only be inserted when this
            condition evaluates to true.
        when_matched_update: Control when existing records
            should be updated. When provided, matching records will only be updated when
            this condition evaluates to true. Source columns can be referenced as
            source.<column_name> and target columns as target.<column_name>.
        when_matched_delete: Control when existing records
            should be deleted. When provided, matching records will be deleted when this
            condition evaluates to true. Source and target cols can be referenced as in
            update conditions.

    Example:
        >>> config = DeltaConfig(
        ...     database="feature_store",
        ...     table="user_features",
        ...     merge_on=["id", "timestamp"],
        ...     deduplicate=True,
        ...     when_matched_update="source.value > target.value",
        ...     when_not_matched_insert="source.value > 0"
        ... )
    """

    database: str
    table: str
    merge_on: List[str]
    deduplicate: bool = False
    when_not_matched_insert: Optional[str] = None
    when_matched_update: Optional[str] = None
    when_matched_delete: Optional[str] = None


class DeltaFeatureStoreWriter(Writer):
    """Enable writing feature sets into Delta tables with merge capabilities.

    Attributes:
        database: database name to use for the Delta table.
        table: table name to write the feature set to.
        merge_on: list of columns to use as merge keys.
        deduplicate: whether to deduplicate data before merging based on featr set keys.
            Default is False.
        when_not_matched_insert: optional condition for insert operations.
            When provided, rows will only be inserted if this condition is true.
        when_matched_update: optional condition for update operations.
            When provided, rows will only be updated if this condition is true.
            Source columns can be referenced as source.<column_name> and target
            columns as target.<column_name>.
        when_matched_delete: optional condition for delete operations.
            When provided, rows will be deleted if this condition is true.
            Source and target columns can be referenced as in update conditions.

    Example:
        Simple example regarding DeltaFeatureStoreWriter class instantiation.
        We can instantiate this class with basic merge configuration:

    >>> from butterfree.load.writers import DeltaFeatureStoreWriter
    >>> spark_client = SparkClient()
    >>> writer = DeltaFeatureStoreWriter(
    ...     database="feature_store",
    ...     table="user_features",
    ...     merge_on=["id", "timestamp"]
    ... )
    >>> writer.write(feature_set=feature_set,
    ...             dataframe=dataframe,
    ...             spark_client=spark_client)

        We can also enable deduplication based on the feature set keys:

    >>> writer = DeltaFeatureStoreWriter(
    ...     database="feature_store",
    ...     table="user_features",
    ...     merge_on=["id", "timestamp"],
    ...     deduplicate=True
    ... )

        For more control over the merge operation, we can add conditions:

    >>> writer = DeltaFeatureStoreWriter(
    ...     database="feature_store",
    ...     table="user_features",
    ...     merge_on=["id", "timestamp"],
    ...     when_matched_update="source.value > target.value",
    ...     when_not_matched_insert="source.value > 0"
    ... )

        The writer supports schema evolution by default and will automatically
        handle updates to the feature set schema.

        When writing with deduplication enabled, the writer will use the feature
        set's key columns and timestamp to ensure data quality by removing
        duplicates before merging.

        For optimal performance, it's recommended to:
        1. Choose appropriate merge keys
        2. Use conditions to filter unnecessary updates/inserts
        3. Enable deduplication only when needed
    """

    def __init__(
        self,
        database: str,
        table: str,
        merge_on: List[str],
        deduplicate: bool = False,
        when_not_matched_insert: Optional[str] = None,
        when_matched_update: Optional[str] = None,
        when_matched_delete: Optional[str] = None,
    ):
        self.config = DeltaConfig(
            database=database,
            table=table,
            merge_on=merge_on,
            deduplicate=deduplicate,
            when_not_matched_insert=when_not_matched_insert,
            when_matched_update=when_matched_update,
            when_matched_delete=when_matched_delete,
        )

    def write(self, dataframe, spark_client, feature_set):
        """Merges the input dataframe into a Delta table.

        Performs a Delta merge operation with the provided dataframe using the config
        merge settings. When deduplication is enabled, uses the feature set's key cols
        to remove duplicates before merging.

        Args:
            dataframe: Spark dataframe with data to be merged.
            spark_client: Client with an active Spark connection.
            feature_set: Feature set instance containing schema and configuration.
                Used for deduplication when enabled.

        Example:
            >>> from butterfree.load.writers import DeltaFeatureStoreWriter
            >>> writer = DeltaFeatureStoreWriter(
            ...     database="feature_store",
            ...     table="user_features",
            ...     merge_on=["id", "timestamp"],
            ...     deduplicate=True
            ... )
            >>> writer.write(
            ...     dataframe=dataframe,
            ...     spark_client=spark_client,
            ...     feature_set=feature_set
            ... )
        """
        DeltaWriter().merge(
            client=spark_client,
            database=self.config.database,
            table=self.config.table,
            merge_on=self.config.merge_on,
            source_df=dataframe,
            feature_set=feature_set if self.config.deduplicate else None,
            deduplicate=self.config.deduplicate,
            when_not_matched_insert=self.config.when_not_matched_insert,
            when_matched_update=self.config.when_matched_update,
            when_matched_delete=self.config.when_matched_delete,
        )
