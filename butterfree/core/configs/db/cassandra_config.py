"""Holds configurations to read and write with Spark to Cassandra DB."""

from butterfree.core.configs import environment
from butterfree.core.configs.db.abstract_config import AbstractWriteConfig


class CassandraConfig(AbstractWriteConfig):
    """Configuration for Spark to connect on Cassandra DB.

    References can be found
    [here](https://docs.databricks.com/data/data-sources/cassandra.html).

    Attributes:
        mode: write mode for Spark.
        format_: write format for Spark.
        keyspace:  Cassandra DB keyspace to write data.
        username: username to use in connection.
        password: password to use in connection.
        host: host to use in connection.
        stream_processing_time: processing time interval for streaming jobs.
        stream_output_mode: specify the mode from writing streaming data.
        stream_checkpoint_path: path on S3 to save checkpoints for the stream job.

    More information about processing_time, output_mode and checkpoint_path
    can be found in Spark documentation:
    [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

    """

    def __init__(
        self,
        mode: str = None,
        format_: str = None,
        keyspace: str = None,
        stream_processing_time: str = None,
        stream_output_mode: str = None,
        stream_checkpoint_path: str = None,
    ):
        self.mode = mode
        self.format_ = format_
        self.keyspace = keyspace
        self.username = environment.get_variable("CASSANDRA_USERNAME")
        self.password = environment.get_variable("CASSANDRA_PASSWORD")
        self.host = environment.get_variable("CASSANDRA_HOST")
        self.stream_processing_time = stream_processing_time
        self.stream_output_mode = stream_output_mode
        self.stream_checkpoint_path = stream_checkpoint_path

    @property
    def format_(self) -> str:
        """Write format for Spark."""
        return self.__format

    @format_.setter
    def format_(self, value: str):
        self.__format = value or "org.apache.spark.sql.cassandra"

    @property
    def mode(self) -> str:
        """Write mode for Spark."""
        return self.__mode

    @mode.setter
    def mode(self, value):
        self.__mode = value or "append"

    @property
    def keyspace(self) -> str:
        """Cassandra DB keyspace to write data."""
        return self.__keyspace

    @keyspace.setter
    def keyspace(self, value: str):
        value = value or environment.get_variable("CASSANDRA_KEYSPACE")
        if not value:
            raise ValueError("Config 'keyspace' cannot be empty.")
        self.__keyspace = value

    @property
    def username(self) -> str:
        """Username used in connection to Cassandra DB."""
        return self.__username

    @username.setter
    def username(self, value: str):
        if value is None:
            raise ValueError("Config 'username' cannot be empty.")
        self.__username = value

    @property
    def password(self) -> str:
        """Password used in connection to Cassandra DB."""
        return self.__password

    @password.setter
    def password(self, value: str):
        if value is None:
            raise ValueError("Config 'password' cannot be empty.")
        self.__password = value

    @property
    def host(self) -> str:
        """Host used in connection to Cassandra DB."""
        return self.__host

    @host.setter
    def host(self, value: str):
        if value is None:
            raise ValueError("Config 'host' cannot be empty.")
        self.__host = value

    @property
    def stream_processing_time(self) -> str:
        """Processing time interval for streaming jobs."""
        return self.__stream_processing_time

    @stream_processing_time.setter
    def stream_processing_time(self, value: str):
        self.__stream_processing_time = value or environment.get_variable(
            "STREAM_PROCESSING_TIME"
        )

    @property
    def stream_output_mode(self) -> str:
        """Specify the mode from writing streaming data."""
        return self.__stream_output_mode

    @stream_output_mode.setter
    def stream_output_mode(self, value: str):
        self.__stream_output_mode = value or "update"

    @property
    def stream_checkpoint_path(self) -> str:
        """Path on S3 to save checkpoints for the stream job."""
        return self.__stream_checkpoint_path

    @stream_checkpoint_path.setter
    def stream_checkpoint_path(self, value: str):
        self.__stream_checkpoint_path = value or environment.get_variable(
            "STREAM_CHECKPOINT_PATH"
        )

    def get_options(self, table: str) -> dict:
        """Get options for connect to Cassandra DB.

        Options will be a dictionary with the write and read configuration for
        spark to cassandra.

        Args:
            table: table name (keyspace) into Cassandra DB.

        Returns:
            Configuration to connect to Cassandra DB.

        """
        return {
            "table": table,
            "keyspace": self.keyspace,
            "spark.cassandra.auth.username": self.username,
            "spark.cassandra.auth.password": self.password,
            "spark.cassandra.connection.host": self.host,
        }
