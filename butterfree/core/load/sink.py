"""Holds the Sink class."""
from typing import List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery

from butterfree.core.clients import SparkClient
from butterfree.core.load.writers.writer import Writer
from butterfree.core.transform import FeatureSet
from butterfree.core.validations.basic_validaton import BasicValidation
from butterfree.core.validations.validation import Validation


class Sink:
    """Define the destinations for the feature set pipeline.

    A Sink is created from a set of writers. The main goal of the Sink is to
    trigger the load in each defined writers. After the load the entity can be
    used to make sure that all data was written properly using the validate
    method.

    Attributes:
        writers: list of Writers to use to load the data.
        validation: validation to check the data before starting to write.

    """

    def __init__(self, writers: List[Writer], validation: Validation = None):
        self.writers = writers
        self.validation = validation

    @property
    def writers(self) -> List[Writer]:
        """List of Writers to use to load the data."""
        return self._writers

    @writers.setter
    def writers(self, value: List[Writer]):
        if not value or not all(isinstance(writer, Writer) for writer in value):
            raise ValueError("Writers needs to be a list of Writer instances.")
        else:
            self._writers = value

    @property
    def validation(self):
        """Validation to check the data before starting to write."""
        return self._validation

    @validation.setter
    def validation(self, value: Validation):
        self._validation = value or BasicValidation()

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
    ) -> List[StreamingQuery]:
        """Trigger a write job in all the defined Writers.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            feature_set: object processed with feature set metadata.
            spark_client: client used to run a query.

        Returns:
            Streaming handlers for each defined writer, if writing streaming dfs.

        """
        self.validation.input(dataframe).check()

        handlers = [
            writer.write(
                feature_set=feature_set, dataframe=dataframe, spark_client=spark_client
            )
            for writer in self.writers
        ]

        return [handler for handler in handlers if handler]
