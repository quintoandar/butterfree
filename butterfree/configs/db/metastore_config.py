"""Holds configurations to read and write with Spark to AWS S3."""

import os
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame

from butterfree.configs import environment
from butterfree.configs.db import AbstractWriteConfig
from butterfree.dataframe_service import extract_partition_values


class MetastoreConfig(AbstractWriteConfig):
    """Configuration for Spark metastore database stored.

    By default the configuration is for AWS S3.

    Attributes:
        path: database root location.
        mode: writing mode used be writers.
        format_: expected stored file format.
        file_system: file schema uri, like: s3a, file.

    """

    def __init__(
        self,
        path: str = None,
        mode: str = None,
        format_: str = None,
        file_system: str = None,
    ):
        self.path = path
        self.mode = mode
        self.format_ = format_
        self.file_system = file_system

    @property
    def path(self) -> Optional[str]:
        """Bucket name."""
        return self.__path

    @path.setter
    def path(self, value: str) -> None:
        self.__path = value or environment.get_variable("FEATURE_STORE_S3_BUCKET")

    @property
    def format_(self) -> Optional[str]:
        """Expected stored file format."""
        return self.__format

    @format_.setter
    def format_(self, value: str) -> None:
        self.__format = value or "parquet"

    @property
    def mode(self) -> Optional[str]:
        """Writing mode used be writers."""
        return self.__mode

    @mode.setter
    def mode(self, value: str) -> None:
        self.__mode = value or "overwrite"

    @property
    def file_system(self) -> Optional[str]:
        """Writing mode used be writers."""
        return self.__file_system

    @file_system.setter
    def file_system(self, value: str) -> None:
        self.__file_system = value or "s3a"

    def get_options(self, key: str) -> Dict[Optional[str], Optional[str]]:
        """Get options for Metastore.

        Options will be a dictionary with the write and read configuration for
        Spark Metastore.

        Args:
            key: path to save data into Metastore.

        Returns:
            Options configuration for Metastore.

        """
        return {
            "mode": self.mode,
            "format_": self.format_,
            "path": os.path.join(f"{self.file_system}://{self.path}/", key),
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
        dataframe_values = extract_partition_values(
            dataframe, partition_columns=["year", "month", "day"]
        )
        for row in dataframe_values:
            path_list.append(
                f"{self.file_system}://{self.path}/{key}/year={row['year']}/"
                f"month={row['month']}/day={row['day']}"
            )

        return path_list

    def translate(self, schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Translate feature set spark schema to the corresponding database."""
        pass
