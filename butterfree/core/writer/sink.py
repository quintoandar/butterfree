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
        spark_client: client for spark connections with external services.
    """

    def __init__(
        self, feature_set: FeatureSet, writers: List[Writer], spark_client: SparkClient
    ):
        self.writers = writers
        self.feature_set = feature_set
        self.spark_client = spark_client

    def validate(self, dataframe: DataFrame):
        """Validate to load the feature set into Writers.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
        """
        check = []
        for writer in self.writers:
            loader = writer(self.spark_client)
            verify = loader.validate(feature_set=self.feature_set, dataframe=dataframe)
            check.append([writer, verify])

        for writer, validate_result in check:
            if validate_result is False:
                raise ValueError("The {} load process was failed.".format(writer))

    def flush(self, dataframe: DataFrame):
        """Loads the data from a feature set into the Feature Store.

        Args:
            dataframe: spark dataframe containing data from a feature set.
        """
        for writer in self.writers:
            writer = writer(self.spark_client)
            writer.write(feature_set=self.feature_set, dataframe=dataframe)
