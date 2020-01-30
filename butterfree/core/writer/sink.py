"""Holds the Sink class."""
from collections import namedtuple
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.core.transform import FeatureSet
from butterfree.core.writer.writer import Writer


class Sink:
    """Run the Writers and validate process checks on the loaded data.

    Attributes:
        writers: list of writers to run.
    """

    def __init__(self, writers: List[Writer]):
        if not writers:
            raise ValueError("The writers list can't be empty.")
        else:
            self.writers = writers

    def validate(self, feature_set: FeatureSet, dataframe: DataFrame):
        """Validate the data loaded by the defined Writers.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            feature_set: object processed with feature_set informations.
        """
        Validation = namedtuple("Validation", ["writer", "result"])

        validations = [
            Validation(
                writer, writer.validate(feature_set=feature_set, dataframe=dataframe),
            )
            for writer in self.writers
        ]
        failures = [validation for validation in validations if not validation.result]

        if failures:
            raise RuntimeError(
                "The following validations returned error: {}".format(failures)
            )

    def flush(self, feature_set: FeatureSet, dataframe: DataFrame):
        """Trigger all the defined Writers into the Feature Store.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            feature_set: object processed with feature_set informations.
        """
        for writer in self.writers:
            writer.write(feature_set=feature_set, dataframe=dataframe)
