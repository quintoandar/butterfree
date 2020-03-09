"""KafkaSource entity."""

from pyspark.sql import DataFrame

from butterfree.core.clients import SparkClient
from butterfree.core.configs import environment
from butterfree.core.extract.readers.reader import Reader


class KafkaReader(Reader):
    """Responsible for get data from a Kafka topic.

    Attributes:
        id: unique string id for register the reader as a view on the metastore
        connection_string: string with hosts and ports to connect. In the
            format: host1:port,host2:port,...,hostN:portN
        topic: string with the Kafka topic name to subscribe.
        topic_options: additional options for consuming from topic. See docs:
            https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html.
        stream: flag to indicate the reading mode: stream or batch

    Example:
        Simple example regarding KafkaReader class instantiation.
    >>> from butterfree.core.extract.readers import KafkaReader
    >>> from butterfree.core.clients import SparkClient
    >>> from butterfree.core.extract.pre_processing import filter
    >>> spark_client = SparkClient()
    >>> kafka_reader = KafkaReader(
    ...                 id="kafka_reader_id",
    ...                 connection_string="host1:port,host2:port",
    ...                 topic="topic"
    ...                )
    >>> df = kafka_reader.consume(spark_client)

        This last method will use the Spark Client, as default, to read
        the desired topic, loading data into a dataframe, according to
        KafkaReader class arguments.

        It's also possible to define simple transformations within the
        reader's scope:
    >>> kafka_reader.with_(filter, condition="year = 2019").build(spark_client)

        In this case, however, a temp view will be created, cointaining
        the transformed data.

    """

    def __init__(
        self,
        id: str,
        topic: str,
        connection_string: str = None,
        topic_options: dict = None,
        stream: bool = True,
    ):
        super().__init__(id)
        if not isinstance(connection_string, str):
            raise ValueError(
                "connection_string must be a string with hosts and ports to connect"
            )
        if not isinstance(topic, str):
            raise ValueError("topic must be a string with the topic name")
        self.topic = topic
        self.connection_string = connection_string or environment.get_variable(
            "KAFKA_CONNECTION_STRING"
        )
        self.options = dict(
            {
                "kafka.bootstrap.servers": self.connection_string,
                "subscribe": self.topic,
            },
            **topic_options if topic_options else {},
        )
        self.stream = stream

    def consume(self, client: SparkClient) -> DataFrame:
        """Extract data from a kafka topic.

        When stream mode it will get all the new data arriving at the topic in a
        streaming dataframe. When not in stream mode it will get all data
        available in the kafka topic.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with

        """
        raw_stream_df = client.read(
            format="kafka", options=self.options, stream=self.stream
        )

        # cast key and value columns from binary to string
        return raw_stream_df.withColumn(
            "key", raw_stream_df["key"].cast("string")
        ).withColumn("value", raw_stream_df["value"].cast("string"))
