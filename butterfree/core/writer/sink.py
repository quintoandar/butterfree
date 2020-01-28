"""Holds the Sink class."""
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.transform import FeatureSet
from butterfree.core.writer.writer import Writer


class Sink:
    """Run the Writers and validate actions them.

    Attributes:
        feature_set: object processed with feature_set informations.
        writers: list of writers to run.
    """

    def __init__(self, feature_set: FeatureSet, writers: List[Writer]):
        self.writers = writers
        self.feature_set = feature_set

    def validate(self, feature_set, dataframe: DataFrame):
        """Validate to load the feature set into Writers.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
        """
        for writer in self.writers:
            writer.validate(feature_set=feature_set, dataframe=dataframe)

    def flush(self, dataframe: DataFrame, spark_client: SparkClient):
        """Loads the data from a feature set into the Feature Store.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            spark_client: ...
        """
        for writer in self.writers:
            writer = writer(spark_client)
            writer.write(feature_set=self.feature_set, dataframe=dataframe)

            if writer.validate(self.feature_set, dataframe) is False:
                raise ValueError("The load process was failed.")
