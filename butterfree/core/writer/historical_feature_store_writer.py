"""Holds the Historical Feature Store writer class."""

import os

from pyspark.sql.dataframe import DataFrame

from butterfree.core.client import SparkClient
from butterfree.core.dataframe.verify_dataframe import VerifyDataframe
from butterfree.core.db.configs import S3Config
from butterfree.core.transform import FeatureSet
from butterfree.core.writer.writer import Writer


class HistoricalFeatureStoreWriter(Writer):
    """Enable writing feature sets into the Historical Feature Store.

    Attributes:
        db_config: configuration with spark for databases or AWS S3 (default).
            For more information access the class in 'butterfree.core.db.configs'.
    """

    def __init__(self, db_config=None):
        self.db_config = db_config or S3Config()

    def write(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Loads the data from a feature set into the Historical Feature Store.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
            spark_client: client for spark connections with external services.
        """
        s3_path = os.path.join(
            self.db_config.path, "historical", feature_set.entity, feature_set.name
        )

        validate_dataframe = VerifyDataframe(dataframe)
        validate_dataframe.checks()

        spark_client.write_table(
            dataframe=dataframe,
            database=self.db_config.database,
            table_name=feature_set.name,
            format_=self.db_config.format_,
            mode=self.db_config.mode,
            partition_by=self.db_config.partition_by,
            path=s3_path,
        )

    def validate(
        self, feature_set: FeatureSet, dataframe: DataFrame, spark_client: SparkClient
    ):
        """Calculate dataframe rows to validate data into Feature Store.

        Args:
            feature_set: object processed with feature_set informations.
            dataframe: spark dataframe containing data from a feature set.
            spark_client: client for spark connections with external services.

        Returns:
            False: fail validation.
            True: success validation.
        """
        table_name = "{}.{}".format(self.db_config.database, feature_set.name)
        query_format_string = "SELECT COUNT(1) as row FROM {}"
        query_count = query_format_string.format(table_name)

        feature_store = spark_client.sql(query=query_count).collect().pop()["row"]

        dataframe = dataframe.count()

        return True if feature_store == dataframe else False
