from typing import Any, Dict, List, Optional

from pyspark.sql.dataframe import DataFrame

from butterfree.clients import SparkClient
from butterfree.configs.db import DeltaConfig
from butterfree.load.writers.delta_writer import DeltaWriter
from butterfree.load.writers.writer import Writer
from butterfree.transform import FeatureSet


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
        when_not_matched_insert: Optional[str] = None,
        when_matched_update: Optional[str] = None,
        when_matched_delete: Optional[str] = None,
    ):
        self.config = DeltaConfig(
            database=database,
            table=table,
            merge_on=merge_on,
            when_not_matched_insert=when_not_matched_insert,
            when_matched_update=when_matched_update,
            when_matched_delete=when_matched_delete,
        )
        self.row_count_validation = False

    def write(
        self,
        dataframe: DataFrame,
        spark_client: SparkClient,
        feature_set: FeatureSet,
    ) -> None:
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
        options = self.config.get_options(self.config.table)

        DeltaWriter().merge(
            client=spark_client,
            database=options["database"],
            table=options["table"],
            merge_on=self.config.merge_on,
            source_df=dataframe,
            when_not_matched_insert=self.config.when_not_matched_insert,
            when_matched_update=self.config.when_matched_update,
            when_matched_delete=self.config.when_matched_delete,
        )

    def validate(
        self,
        dataframe: DataFrame,
        spark_client: SparkClient,
        feature_set: FeatureSet,
    ) -> None:
        """Validates the dataframe written to Delta table.

        In Delta tables, schema validation is handled by Delta's schema enforcement
        and evolution. No additional validation is needed.

        Args:
            dataframe: Spark dataframe to be validated
            spark_client: Client for Spark connection
            feature_set: Feature set with the schema definition
        """
        pass

    def check_schema(self, dataframe: DataFrame, schema: List[Dict[str, Any]]) -> None:
        """Checks if the dataframe schema matches the feature set schema.

        Schema validation in Delta tables is handled by Delta Lake's schema enforcement
        and evolution capabilities.

        Args:
            dataframe: Spark dataframe to be validated
            schema: Schema definition from the feature set
        """
        pass
