"""Holds the Historical Feature Store writer class."""

import os

from butterfree.core.dataframe.verify_dataframe import VerifyDataframe
from butterfree.core.db.configs import S3Config
from butterfree.core.transform import FeatureSet
from butterfree.core.writer.writer import Writer


class HistoricalFeatureStoreWriter(Writer):
    """Enable writing feature sets into the Historical Feature Store.

    Attributes:
        spark_client: client for spark connections with external services.
    """

    def __init__(self, spark_client, db_config=None):
        super().__init__(spark_client)
        self.db_config = db_config or S3Config()

    def write(self, feature_set: FeatureSet, dataframe):
        """Loads the data from a feature set into the Historical Feature Store.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
        """
        s3_path = os.path.join(self.db_config.path, feature_set.name)

        validate_dataframe = VerifyDataframe(dataframe)
        validate_dataframe.checks()

        self.spark_client.write_table(
            dataframe=dataframe,
            database=self.db_config.database,
            table_name=feature_set.name,
            format_=self.db_config.format_,
            mode=self.db_config.mode,
            partition_by=self.db_config.partition_by,
            path=s3_path,
        )

    def validate(self, feature_set: FeatureSet, dataframe):
        """Validate to load the feature set into Writer.

        Args:
            dataframe: spark dataframe containing data from a feature set.
            format: string with the file format.
            path: local where feature set was saved.

        Returns:
            False: fail validation.
            True: success validation.
        """
        if not isinstance(self.db_config.format_, str):
            raise ValueError("format needs to be a string with the desired read format")
        if not isinstance(self.db_config.path, str):
            raise ValueError(
                "path needs to be a string with the local of the registered table"
            )

        s3_path = os.path.join(self.db_config.path, feature_set.name)

        feature_store = self.spark_client.read(
            format=self.db_config.format_, options={"path": s3_path}
        ).count()
        dataframe = dataframe.count()

        return True if feature_store == dataframe else False
