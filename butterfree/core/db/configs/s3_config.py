"""Holds configurations to read and write with Spark to AWS S3."""

from butterfree.core.configs import environment
from butterfree.core.db.configs.abstract_config import AbstractWriteConfig


class S3Config(AbstractWriteConfig):
    """Configuration for Spark metastore database stored on AWS S3.

    Attributes:
        database: database name.
        mode: writing mode used be writers.
        format_: expected stored file format.
        path: database root location.
        partition_by: partition column to use when writing.

    """

    def __init__(
        self,
        database: str = None,
        mode: str = None,
        format_: str = None,
        path: str = None,
        partition_by: str = None,
    ):
        self.database = database
        self.mode = mode
        self.format_ = format_
        self.path = path
        self.partition_by = partition_by
        self.feature_name = environment.get_variable("FEATURE_STORE_S3_BUCKET")

    @property
    def database(self) -> str:
        """Database name."""
        return self.__database

    @database.setter
    def database(self, value: str):
        self.__database = value or "feature_store"

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

    @property
    def path(self) -> str:
        """Database root location."""
        return self.__path

    @path.setter
    def path(self, value):
        self.__path = (
            value or f"s3a://{environment.get_variable('FEATURE_STORE_S3_BUCKET')}"
        )

    @property
    def partition_by(self) -> str:
        """Partition column to use when writing."""
        return self.__partition_by

    @partition_by.setter
    def partition_by(self, value):
        self.__partition_by = value or [
            "partition__year",
            "partition__month",
            "partition__day",
        ]

    def get_options(self, table: str) -> dict:
        """Get options for AWS S3.

        Options will be a dictionary with the write and read configuration for
        Spark to AWS S3.

        Args:
            table: table name into AWS S3.

        Returns:
            Options configuration for AWS S3.

        """
        return {
            "table": table,
        }
