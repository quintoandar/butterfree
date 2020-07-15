"""Reader entity."""

from abc import ABC, abstractmethod
from functools import reduce
from typing import Callable, List

from pyspark.sql import DataFrame

from butterfree.clients import SparkClient


class Reader(ABC):
    """Abstract base class for Readers.

    Attributes:
        id: unique string id for register the reader as a view on the metastore.
        transformations: list os methods that will be applied over the dataframe
            after the raw data is extracted.

    """

    def __init__(self, id: str):
        self.id = id
        self.transformations = []

    def with_(self, transformer: Callable, *args, **kwargs):
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

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        return reduce(
            lambda result_df, transformation: transformation["transformer"](
                result_df, *transformation["args"], **transformation["kwargs"]
            ),
            self.transformations,
            df,
        )

    @abstractmethod
    def consume(self, client: SparkClient) -> DataFrame:
        """Extract data from target origin.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with all the data.

        :return: Spark dataframe
        """

    def build(self, client: SparkClient, columns: List[tuple] = None):
        """Register the data got from the reader in the Spark metastore.

        Create a temporary view in Spark metastore referencing the data
        extracted from the target origin after the application of all the
        defined pre-processing transformations.

        Args:
            client: client responsible for connecting to Spark session.
            columns: list of tuples for renaming/filtering the dataset.

        """
        transformed_df = self._apply_transformations(self.consume(client))

        if columns:
            select_expression = []
            for old_expression, new_column_name in columns:
                select_expression.append(f"{old_expression} as {new_column_name}")
            transformed_df = transformed_df.selectExpr(*select_expression)

        transformed_df.createOrReplaceTempView(self.id)
