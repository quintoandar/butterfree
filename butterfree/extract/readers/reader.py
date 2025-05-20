"""Reader entity."""

from abc import ABC, abstractmethod
from functools import reduce
from typing import Any, Callable, Dict, List, Optional, Type

from pyspark.sql import DataFrame

from butterfree.clients import SparkClient
from butterfree.dataframe_service import IncrementalStrategy
from butterfree.extract.readers.reader_metadata import (
    BaseReaderMetadata,
    FileReaderMetadata,
    KafkaReaderMetadata,
    TableReaderMetadata,
)
from butterfree.hooks import HookableComponent


class Reader(ABC, HookableComponent):
    """Abstract base class for Readers.

    Attributes:
        id: unique string id for register the reader as a view.
        transformations: list os methods that will be applied over the dataframe
            after the raw data is extracted.

    """

    def __init__(
        self, id: str, incremental_strategy: Optional[IncrementalStrategy] = None
    ):
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
        columns: Optional[List[Any]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
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

    def get_metadata(self) -> BaseReaderMetadata:
        """Get the reader's metadata as a Pydantic model.

        This method creates a standardized representation of reader metadata
        that can be used for documentation, validation, and serialization purposes.
        Each reader type (File, Kafka, Table) has its own specific metadata
        while sharing common base attributes.

        Returns:
            A BaseReaderMetadata model containing the reader's metadata
        """
        reader_type_map: Dict[str, Type[BaseReaderMetadata]] = {
            "FileReader": FileReaderMetadata,
            "KafkaReader": KafkaReaderMetadata,
            "TableReader": TableReaderMetadata,
        }

        reader_type = self._get_reader_type()

        reader_metadata = {
            "id": self.id,
            "type": reader_type,
            "incremental_strategy": self.incremental_strategy is not None,
            "stream": getattr(self, "stream", False),
            **self._get_reader_specific_metadata(),
        }

        config_model = reader_type_map.get(reader_type)
        if not config_model:
            raise ValueError(f"No metadata model found for reader type: {reader_type}")

        return config_model(**reader_metadata)

    def _get_reader_type(self) -> str:
        """Get the standardized reader type name.

        Returns:
            A string representing the reader type (FileReader, KafkaReader, or TableReader)  # noqa: E501
        """
        return self.__class__.__name__

    def _get_reader_specific_metadata(self) -> dict:
        """Get reader-specific metadata.

        This method should be overridden by specific reader implementations
        to provide their unique metadata.

        Returns:
            A dictionary containing reader-specific metadata
        """
        return {}
