"""Holds the Sink class."""
from collections import namedtuple
from typing import List

from pyspark.sql.dataframe import DataFrame

from butterfree.core.transform import FeatureSet
from butterfree.core.writer.writer import Writer


class Sink:
    """Run the Writers and validate process checks on the loaded data.

    Attributes:
        feature_set: object processed with feature_set informations.
        writers: list of writers to run.
    """

    def __init__(self, feature_set: FeatureSet, writers: List[Writer]):
        self.writers = writers
        self.feature_set = feature_set

    def validate(self, dataframe: DataFrame):
        """Validate the data loaded by the defined Writers.

        Args:
            dataframe: spark dataframe containing data from a feature set.
        """
        Validation = namedtuple("Validation", ["writer", "result"])

        validations = [
            Validation(
                writer,
                writer.validate(feature_set=self.feature_set, dataframe=dataframe),
            )
            for writer in self.writers
        ]
        failures = [validation for validation in validations if not validation[1]]

        if failures:
            raise RuntimeError(
                "The following validations returned error: {}".format(failures)
            )

    def flush(self, dataframe: DataFrame):
        """Trigger all the defined Writers into the Feature Store.

        Args:
            dataframe: spark dataframe containing data from a feature set.
        """
        for writer in self.writers:
            writer.write(feature_set=self.feature_set, dataframe=dataframe)
