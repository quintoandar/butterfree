"""Holds the Sink class."""
from typing import List, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.streaming import StreamingQuery

from butterfree.clients import SparkClient
from butterfree.hooks import HookableComponent
from butterfree.load.writers.writer import Writer
from butterfree.transform import FeatureSet
from butterfree.validations import BasicValidation
from butterfree.validations.validation import Validation


class Sink(HookableComponent):
    """Define the destinations for the feature set pipeline.

    A Sink is created from a set of writers. The main goal of the Sink is to
    trigger the load in each defined writers. After the load the entity can be
    used to make sure that all data was written properly using the validate
    method.

    Attributes:
        writers: list of Writers to use to load the data.
        validation: validation to check the data before starting to write.

    """

    def __init__(self, writers: List[Writer], validation: Optional[Validation] = None):
        super().__init__()
        self.enable_post_hooks = False
        self.writers = writers
        self.validation = validation

    @property
    def writers(self) -> List[Writer]:
        """List of Writers to use to load the data."""
        return self._writers

    @writers.setter
    def writers(self, value: List[Writer]) -> None:
        if not value or not all(isinstance(writer, Writer) for writer in value):
            raise ValueError("Writers needs to be a list of Writer instances.")
        else:
            self._writers = value

    @property
    def validation(self) -> Optional[Validation]:
        """Validation to check the data before starting to write."""
        return self._validation

    @validation.setter
    def validation(self, value: Validation) -> None:
        self._validation = value or BasicValidation()

    def validate(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ) -> None:
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
        pre_hook_df = self.run_pre_hooks(dataframe)

        if self.validation is not None:
            self.validation.input(pre_hook_df).check()

        handlers = [
            writer.write(
                feature_set=feature_set,
                dataframe=pre_hook_df,
                spark_client=spark_client,
            )
            for writer in self.writers
        ]

        return [handler for handler in handlers if handler]
