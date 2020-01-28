"""Holds configurations to read and write with Spark to AWS S3."""

from butterfree.core.configs import environment
from butterfree.core.db.configs.abstract_config import AbstractWriteConfig


class S3Config(AbstractWriteConfig):
    """Configuration with spark for AWS S3.

    Attributes:
        database: specified database name.
        mode: write mode for spark.
        format_: string with the file format.
        path: string with the local to save the feature set.
        partition_by: names of partitioning columns.
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
        """Attribute "database" getter.

        :return database: write database for S3.
        """
        return self.__database

    @database.setter
    def database(self, value: str):
        """Attribute "database" setter.

        :param value: used to set attribute "database".
        """
        self.__database = value or "feature_store"

    @property
    def format_(self) -> str:
        """Attribute "format" getter.

        :return format_: write format for spark with S3.
        """
        return self.__format

    @format_.setter
    def format_(self, value: str):
        """Attribute "format" setter.

        :param value: used to set attribute "format_".
        """
        self.__format = value or "parquet"

    @property
    def mode(self) -> str:
        """Attribute "mode" getter.

        :return mode: write mode for spark with S3.
        """
        return self.__mode

    @mode.setter
    def mode(self, value):
        """Attribute "mode" setter.

        :param value: used to set attribute "mode".
        """
        self.__mode = value or "overwrite"

    @property
    def path(self) -> str:
        """Attribute "path" getter.

        :return path: write path for spark with S3.
        """
        return self.__path

    @path.setter
    def path(self, value):
        """Attribute "path" setter.

        :param value: used to set attribute "path".
        """
        self.__path = (
            value or f"s3a://{environment.get_variable('FEATURE_STORE_S3_BUCKET')}"
        )

    @property
    def partition_by(self) -> str:
        """Attribute "partition_by" getter.

        :return path: write partition_by for spark with S3.
        """
        return self.__partition_by

    @partition_by.setter
    def partition_by(self, value):
        """Attribute "partition_by" setter.

        :param value: used to set attribute "partition_by".
        """
        self.__partition_by = value or [
            "partition__year",
            "partition__month",
            "partition__day",
        ]

    def get_options(self, table: str) -> dict:
        """Get options.

        Options will be a dictionary with the write and read configuration for spark to
        AWS S3.

        :param table: table name into AWS S3.
        :return: dict of the params to S3.
        """
        return {
            "table": table,
        }
