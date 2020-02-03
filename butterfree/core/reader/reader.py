"""Reader entity."""

from abc import ABC, abstractmethod
from functools import reduce

from pyspark.sql import DataFrame


class Reader(ABC):
    """Abstract base class for Readers.

    Attributes:
        :param id: unique string id for register the reader as a view on the
            metastore
        :param client: client object from butterfree.core.client module
        :param transformations: list os methods that will be applied over the
            dataframe after the raw data is extracted

    """

    def __init__(self, id: str, client):
        self.id = id
        self.client = client
        self.transformations = []

    def with_(self, transformer, *args, **kwargs) -> "Reader":
        """Define a new transformation for the Reader.

        All the transformations are used when the method consume is called.

        Args:
            transformer: method that receives a dataframe and output a dataframe
            *args: args for the transformer
            **kwargs: kwargs for the transformer

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
        """Apply all the transformations defined over a passed Spark dataframe.

        Args:
            df: Spark dataframe to be pre-processed

        Returns:
            Transformed dataframe

        """
        return reduce(
            lambda result_df, transformation: transformation["transformer"](
                result_df, *transformation["args"], **transformation["kwargs"]
            ),
            self.transformations,
            df,
        )

    @abstractmethod
    def consume(self) -> DataFrame:
        """Extracts data from from defined origin."""

    def build(self):
        """Register the data got from the reader in the Spark metastore.

        Create a temporary view in Spark metastore referencing the data
        extracted from the defined Reader, using the consume method.

        """
        self._apply_transformations(self.consume()).createOrReplaceTempView(self.id)
