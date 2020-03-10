"""Holds the Sink class."""
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.core.clients import SparkClient
from butterfree.core.load.writers.writer import Writer
from butterfree.core.transform import FeatureSet


class Sink:
    """Define the destinations for the feature set pipeline.

    A Sink is created from a set of writers. The main goal of the Sink is to
    trigger the load in each defined writers. After the load the entity can be
    used to make sure that all data was written properly using the validate
    method.

    Attributes:
        writers: list of writers to run.

    """

    def __init__(self, writers: List[Writer]):
        if not writers:
            raise ValueError("The writers list can't be empty.")
        else:
            self.writers = writers

    def validate(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Trigger a validation job in all the defined Writers.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            feature_set: object processed with feature set metadata.
            spark_client: client used to run a query.

        Raises:
            RuntimeError: if any on the Writers returns a failed validation.

        """
        failures = []
        for writer in self.writers:
            try:
                writer.validate(
                    feature_set=feature_set,
                    dataframe=dataframe,
                    spark_client=spark_client,
                )
            except AssertionError as e:
                failures.append(e)

        if failures:
            raise RuntimeError(
                "The following validations returned error: {}".format(failures)
            )

    def flush(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Trigger a write job in all the defined Writers.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            feature_set: object processed with feature set metadata.
            spark_client: client used to run a query.

        """
        for writer in self.writers:
            writer.write(
                feature_set=feature_set, dataframe=dataframe, spark_client=spark_client
            )
