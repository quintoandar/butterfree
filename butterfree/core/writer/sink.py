"""Holds the Sink  class."""
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.core.transform import FeatureSet
from butterfree.core.writer.writer import Writer


class Sink:
    """Run the Writers and validate actions them.

    Attributes:
        feature_set: ...
        writers: list of writers to run.
    """

    def __init__(self, feature_set: FeatureSet, writers: List[Writer]):
        self.writers = writers
        self.feature_set = feature_set

    def validate(self, dataframe: DataFrame):
        """Validate to load the feature set into Writers.

        Args:
            dataframe: ...
        """
        for writer in self.writers:
            writer.validate(dataframe)

    def flush(self, dataframe: DataFrame):
        """Loads the data from a feature set into the Feature Store.

        Args:
            dataframe: ...
        """
        for writer in self.writers:
            writer.write(dataframe=dataframe, feature_set=self.feature_set)

        if not self.validate(dataframe):
            raise ValueError("Dataframe is invalid.")
