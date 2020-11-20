"""Holds configurations to read and write with Spark to Kafka."""
from typing import Dict, List

from butterfree.configs import environment
from butterfree.configs.db import AbstractWriteConfig


class KafkaConfig(AbstractWriteConfig):
    """Configuration for Spark to connect to Kafka.

    Attributes:
        kafka_connection_string: string with hosts and ports to connect.
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
        kafka_connection_string: str = None,
        mode: str = None,
        format_: str = None,
        stream_processing_time: str = None,
        stream_output_mode: str = None,
        stream_checkpoint_path: str = None,
    ):
        self.kafka_connection_string = kafka_connection_string
        self.mode = mode
        self.format_ = format_
        self.stream_processing_time = stream_processing_time
        self.stream_output_mode = stream_output_mode
        self.stream_checkpoint_path = stream_checkpoint_path

    @property
    def kafka_connection_string(self) -> str:
        """Username used in connection to Cassandra DB."""
        return self.__kafka_connection_string

    @kafka_connection_string.setter
    def kafka_connection_string(self, value: str):
        input_value = value or environment.get_variable(
            "KAFKA_CONSUMER_CONNECTION_STRING"
        )
        if input_value is None:
            raise ValueError("Config 'kafka connection string' cannot be empty.")
        self.__kafka_connection_string = input_value

    @property
    def mode(self) -> str:
        """Write mode for Spark."""
        return self.__mode

    @mode.setter
    def mode(self, value):
        self.__mode = value or "append"

    @property
    def format_(self) -> str:
        """Write format for Spark."""
        return self.__format

    @format_.setter
    def format_(self, value: str):
        self.__format = value or "kafka"

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

    @property
    def stream_processing_time(self) -> str:
        """Processing time interval for streaming jobs."""
        return self.__stream_processing_time

    @stream_processing_time.setter
    def stream_processing_time(self, value: str):
        self.__stream_processing_time = value or "0 seconds"

    def get_options(self, topic: str) -> dict:
        """Get options for connecting to Kafka.

        Options will be a dictionary with the write and read configuration for
        spark to kafka.

        Args:
            topic: topic related to Kafka.

        Returns:
            Configuration to connect to Kafka.

        """
        return {
            "topic": topic,
            "kafka.bootstrap.servers": self.kafka_connection_string,
        }

    def translate(self, schema) -> List[Dict]:
        """Get feature set schema to be translated.

        The output will be a list of dictionaries regarding cassandra
        database schema.

        Args:
            schema: feature set schema in spark.

        Returns:
            Kafka schema.

        """
        pass
