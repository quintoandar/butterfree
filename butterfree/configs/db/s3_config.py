"""Holds configurations to read and write with Spark to AWS S3."""

import os
from typing import Dict, List

from pyspark.sql import DataFrame

from butterfree.configs import environment
from butterfree.configs.db import AbstractWriteConfig


class S3Config(AbstractWriteConfig):
    """Configuration for Spark metastore database stored on AWS S3.

    Attributes:
        bucket: AWS S3 bucket name.
        mode: writing mode used be writers.
        format_: expected stored file format.

    """

    def __init__(
        self, bucket: str = None, mode: str = None, format_: str = None,
    ):
        self.bucket = bucket
        self.mode = mode
        self.format_ = format_

    @property
    def bucket(self) -> str:
        """Bucket name."""
        return self.__bucket

    @bucket.setter
    def bucket(self, value: str):
        self.__bucket = value or environment.get_variable("FEATURE_STORE_S3_BUCKET")

    @property
    def format_(self) -> str:
        """Expected stored file format."""
        return self.__format

    @format_.setter
    def format_(self, value: str):
        self.__format = value or "parquet"

    @property
    def mode(self) -> str:
        """Writing mode used be writers."""
        return self.__mode

    @mode.setter
    def mode(self, value):
        self.__mode = value or "overwrite"

    def get_options(self, key: str) -> dict:
        """Get options for AWS S3.

        Options will be a dictionary with the write and read configuration for
        Spark to AWS S3.

        Args:
            key: path to save data into AWS S3 bucket.

        Returns:
            Options configuration for AWS S3.

        """
        return {
            "mode": self.mode,
            "format_": self.format_,
            "path": os.path.join(f"s3a://{self.bucket}/", key),
        }

    def get_path_with_partitions(self, key: str, dataframe: DataFrame) -> List:
        """Get options for AWS S3 from partitioned parquet file.

        Options will be a dictionary with the write and read configuration for
        Spark to AWS S3.

        Args:
            key: path to save data into AWS S3 bucket.
            dataframe: spark dataframe containing data from a feature set.

        Returns:
            A list of string for file-system backed data sources.
        """
        path_list = []
        for row in dataframe.collect():
            path = (
                f"s3a://{self.bucket}/{key}/year={row['year']}/"
                f"month={row['month']}/day={row['day']}"
            )
            if path not in path_list:
                path_list.append(path)

        return path_list

    def translate(self, schema) -> List[Dict]:
        """Translate feature set spark schema to the corresponding database."""
        pass
