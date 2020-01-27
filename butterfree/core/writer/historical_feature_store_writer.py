"""Holds the Historical Feature Store writer class."""

import os

from butterfree.core.configs import environment
from butterfree.core.dataframe.verify_dataframe import VerifyDataframe
from butterfree.core.writer.writer import Writer


class HistoricalFeatureStoreWriter(Writer):
    """Enable writing feature sets into the Historical Feature Store.

    Attributes:
        spark_client: client for spark connections with external services.
    """

    HISTORICAL_FEATURE_STORE_S3_PATH = (
        f"s3a://{environment.get_variable('FEATURE_STORE_S3_BUCKET')}/historical/"
    )
    DEFAULT_DATABASE = "feature_store"
    DEFAULT_FORMAT = "parquet"
    DEFAULT_MODE = "overwrite"
    DEFAULT_PARTITION_BY = ["partition__year", "partition__month", "partition__day"]

    def __init__(self, spark_client):
        super().__init__(spark_client)

    def write(self, feature_set, dataframe):
        """Loads the data from a feature set into the Historical Feature Store.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
        """
        s3_path = os.path.join(
            self.HISTORICAL_FEATURE_STORE_S3_PATH, feature_set["name"]
        )

        validate_dataframe = VerifyDataframe(dataframe)
        validate_dataframe.checks()

        self.spark_client.write_table(
            dataframe=dataframe,
            database=self.DEFAULT_DATABASE,
            table_name=feature_set["name"],
            format_=self.DEFAULT_FORMAT,
            mode=self.DEFAULT_MODE,
            partition_by=self.DEFAULT_PARTITION_BY,
            path=s3_path,
        )

    def validate(self, feature_set, dataframe):
        """Validate to load the feature set into Writer.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            format: string with the file format
            path: local where feature set was saved.

        Returns:
            False: fail validation.
            True: success validation.
        """
        if not isinstance(feature_set["format"], str):
            raise ValueError("format needs to be a string with the desired read format")
        if not isinstance(feature_set["path"], str):
            raise ValueError(
                "path needs to be a string with the local of the registered table"
            )

        feature_store = self.spark_client.read(
            format=feature_set["format"], options={"path": feature_set["path"]}
        ).count()
        dataframe = dataframe.count()

        return True if feature_store == dataframe else False
