"""Holds configurations to read and write with Spark to Cassandra DB."""
from typing import Any, Dict, List, Optional

from butterfree.configs import environment
from butterfree.configs.db import AbstractWriteConfig


class CassandraConfig(AbstractWriteConfig):
    """Configuration for Spark to connect on Cassandra DB.

    References can be found
    [here](https://docs.databricks.com/data/data-sources/cassandra.html).

    Attributes:
        username: username to use in connection.
        password: password to use in connection.
        host: host to use in connection.
        keyspace:  Cassandra DB keyspace to write data.
        mode: write mode for Spark.
        format_: write format for Spark.
        stream_processing_time: processing time interval for streaming jobs.
        stream_output_mode: specify the mode from writing streaming data.
        stream_checkpoint_path: path on S3 to save checkpoints for the stream job.

    More information about processing_time, output_mode and checkpoint_path
    can be found in Spark documentation:
    [here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

    """

    def __init__(
        self,
        username: str = None,
        password: str = None,
        host: str = None,
        keyspace: str = None,
        mode: str = None,
        format_: str = None,
        stream_processing_time: str = None,
        stream_output_mode: str = None,
        stream_checkpoint_path: str = None,
    ):
        self.username = username
        self.password = password
        self.host = host
        self.keyspace = keyspace
        self.mode = mode
        self.format_ = format_
        self.stream_processing_time = stream_processing_time
        self.stream_output_mode = stream_output_mode
        self.stream_checkpoint_path = stream_checkpoint_path

    @property
    def username(self) -> Optional[str]:
        """Username used in connection to Cassandra DB."""
        return self.__username

    @username.setter
    def username(self, value: str) -> None:
        input_value = value or environment.get_variable("CASSANDRA_USERNAME")
        if input_value is None:
            raise ValueError("Config 'username' cannot be empty.")
        self.__username = input_value

    @property
    def password(self) -> Optional[str]:
        """Password used in connection to Cassandra DB."""
        return self.__password

    @password.setter
    def password(self, value: str) -> None:
        input_value = value or environment.get_variable("CASSANDRA_PASSWORD")
        if input_value is None:
            raise ValueError("Config 'password' cannot be empty.")
        self.__password = input_value

    @property
    def host(self) -> Optional[str]:
        """Host used in connection to Cassandra DB."""
        return self.__host

    @host.setter
    def host(self, value: str) -> None:
        input_value = value or environment.get_variable("CASSANDRA_HOST")
        if input_value is None:
            raise ValueError("Config 'host' cannot be empty.")
        self.__host = input_value

    @property
    def keyspace(self) -> Optional[str]:
        """Cassandra DB keyspace to write data."""
        return self.__keyspace

    @keyspace.setter
    def keyspace(self, value: str) -> None:
        input_value = value or environment.get_variable("CASSANDRA_KEYSPACE")
        if not input_value:
            raise ValueError("Config 'keyspace' cannot be empty.")
        self.__keyspace = input_value

    @property
    def format_(self) -> Optional[str]:
        """Write format for Spark."""
        return self.__format

    @format_.setter
    def format_(self, value: str) -> None:
        self.__format = value or "org.apache.spark.sql.cassandra"

    @property
    def mode(self) -> Optional[str]:
        """Write mode for Spark."""
        return self.__mode

    @mode.setter
    def mode(self, value: str) -> None:
        self.__mode = value or "append"

    @property
    def stream_processing_time(self) -> Optional[str]:
        """Processing time interval for streaming jobs."""
        return self.__stream_processing_time

    @stream_processing_time.setter
    def stream_processing_time(self, value: str) -> None:
        self.__stream_processing_time = value or "0 seconds"

    @property
    def stream_output_mode(self) -> Optional[str]:
        """Specify the mode from writing streaming data."""
        return self.__stream_output_mode

    @stream_output_mode.setter
    def stream_output_mode(self, value: str) -> None:
        self.__stream_output_mode = value or "update"

    @property
    def stream_checkpoint_path(self) -> Optional[str]:
        """Path on S3 to save checkpoints for the stream job."""
        return self.__stream_checkpoint_path

    @stream_checkpoint_path.setter
    def stream_checkpoint_path(self, value: str) -> None:
        self.__stream_checkpoint_path = value or environment.get_variable(
            "STREAM_CHECKPOINT_PATH"
        )

    def get_options(self, table: str) -> Dict[Optional[str], Optional[str]]:
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

    def translate(self, schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get feature set schema to be translated.

        The output will be a list of dictionaries regarding cassandra
        database schema.

        Args:
            schema: feature set schema in spark.

        Returns:
            Cassandra schema.

        """
        cassandra_mapping = {
            "TimestampType": "timestamp",
            "BinaryType": "boolean",
            "BooleanType": "boolean",
            "DateType": "timestamp",
            "DecimalType": "decimal",
            "DoubleType": "double",
            "FloatType": "float",
            "IntegerType": "int",
            "LongType": "bigint",
            "StringType": "text",
            "ArrayType(LongType,true)": "frozen<list<bigint>>",
            "ArrayType(StringType,true)": "frozen<list<text>>",
            "ArrayType(FloatType,true)": "frozen<list<float>>",
        }
        cassandra_schema = []
        for features in schema:
            cassandra_schema.append(
                {
                    "column_name": features["column_name"],
                    "type": cassandra_mapping[str(features["type"])],
                    "primary_key": features["primary_key"],
                }
            )

        return cassandra_schema
