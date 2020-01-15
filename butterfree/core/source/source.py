"""Source entity."""

from abc import ABC, abstractmethod
from functools import reduce


class Source(ABC):
    """Abstract base class for Sources."""

    @abstractmethod
    def __init__(self, id, client, consume_options, transformations):
        """Instantiate Source with the required parameters.

        :param id: unique string id for register the source as a view on the metastore
        :param client: client object from butterfree.core.client module
        :param consume_options: dict with the necessary configuration to be used in
        order for the data to be consumed.
        :param transformations: list os methods that will be applied over the dataframe
        after the raw data is extracted
        """
        self.id = id
        self.client = client
        self.consume_options = consume_options
        self.transformations = transformations if transformations else []

    def with_(self, transformer, *args, **kwargs):
        """Define a new transformation for the Source.

        All the transformations are used when the method consume is called.
        :param transformer: method that receives a Spark dataframe and output a
        Spark dataframe
        :param args: args for the method
        :param kwargs: kwargs for the method
        :return:
        """
        new_transformation = [
            {"transformer": transformer, "args": args, "kwargs": kwargs}
        ]
        new_parameters = dict(
            (key, val + new_transformation) if key == "transformations" else (key, val)
            for key, val in self.__dict__.items()
        )
        return self.__class__(**new_parameters)

    def _apply_transformations(self, df):
        """Apply all the transformations defined over a passed Spark dataframe.

        :param df: Spark dataframe to be pre-processed
        :return: Spark dataframe
        """
        return reduce(
            lambda result_df, transformation: transformation["transformer"](
                result_df, *transformation["args"], **transformation["kwargs"]
            ),
            self.transformations,
            df,
        )

    @abstractmethod
    def consume(self):
        """Extract data from source using the consume_options defined in the build."""

    def build(self):
        """Register the source in the Spark metastore.

        Create a temporary view in Spark metastore referencing the data extracted from
        the defined Source, using the consume method
        :return: None
        """
        self.consume().createOrReplaceTempView(self.id)
