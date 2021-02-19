"""Reader entity."""

from abc import ABC, abstractmethod
from functools import reduce
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame

from butterfree.clients import SparkClient
from butterfree.dataframe_service import IncrementalStrategy
from butterfree.hooks import HookableComponent


class Reader(ABC, HookableComponent):
    """Abstract base class for Readers.

    Attributes:
        id: unique string id for register the reader as a view on the metastore.
        transformations: list os methods that will be applied over the dataframe
            after the raw data is extracted.

    """

    def __init__(self, id: str, incremental_strategy: IncrementalStrategy = None):
        super().__init__()
        self.id = id
        self.transformations: List[Dict[str, Any]] = []
        self.incremental_strategy = incremental_strategy

    def with_(
        self, transformer: Callable[..., DataFrame], *args: Any, **kwargs: Any
    ) -> Any:
        """Define a new transformation for the Reader.

        All the transformations are used when the method consume is called.

        Args:
            transformer: method that receives a dataframe and output a
                dataframe.
            *args: args for the transformer.
            **kwargs: kwargs for the transformer.

        Returns:
            Reader object with new transformation

        """
        new_transformation = {
            "transformer": transformer,
            "args": args if args else (),
            "kwargs": kwargs if kwargs else {},
        }
        self.transformations.append(new_transformation)
        return self

    def with_incremental_strategy(
        self, incremental_strategy: IncrementalStrategy
    ) -> "Reader":
        """Define the incremental strategy for the Reader.

        Args:
            incremental_strategy: definition of the incremental strategy.

        Returns:
            Reader with defined incremental strategy.
        """
        self.incremental_strategy = incremental_strategy
        return self

    @abstractmethod
    def consume(self, client: SparkClient) -> DataFrame:
        """Extract data from target origin.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with all the data.

        :return: Spark dataframe
        """

    def build(
        self,
        client: SparkClient,
        columns: List[Any] = None,
        start_date: str = None,
        end_date: str = None,
    ) -> None:
        """Register the data got from the reader in the Spark metastore.

        Create a temporary view in Spark metastore referencing the data
        extracted from the target origin after the application of all the
        defined pre-processing transformations.

        The arguments start_date and end_date are going to be use only when there
        is a defined `IncrementalStrategy` on the `Reader`.

        Args:
            client: client responsible for connecting to Spark session.
            columns: list of tuples for selecting/renaming columns on the df.
            start_date: lower bound to use in the filter expression.
            end_date: upper bound to use in the filter expression.

        """
        column_selection_df = self._select_columns(columns, client)
        transformed_df = self._apply_transformations(column_selection_df)

        if self.incremental_strategy:
            transformed_df = self.incremental_strategy.filter_with_incremental_strategy(
                transformed_df, start_date, end_date
            )

        post_hook_df = self.run_post_hooks(transformed_df)

        post_hook_df.createOrReplaceTempView(self.id)

    def _select_columns(
        self, columns: Optional[List[Any]], client: SparkClient
    ) -> DataFrame:
        df = self.consume(client)
        return df.selectExpr(
            *(
                [
                    f"{old_expression} as {new_column_name}"
                    for old_expression, new_column_name in columns
                ]
                if columns
                else df.columns
            )
        )

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        return reduce(
            lambda result_df, transformation: transformation["transformer"](
                result_df, *transformation["args"], **transformation["kwargs"]
            ),
            self.transformations,
            df,
        )
