"""KafkaSource entity."""

from butterfree.core.reader.reader import Reader


class KafkaReader(Reader):
    """Reader responsible for get data from a Kafka topic."""

    def __init__(
        self,
        id,
        spark_client,
        connection_string,
        topic,
        topic_options=None,
        stream=True,
    ):
        """Instantiate KafkaReader with the required parameters.

        :param id: unique string id for register the reader as a view on the metastore
        :param spark_client: spark_client object client module
        :param connection_string: string with hosts and ports to connect. In the format:
        host1:port,host2:port,...,host:port
        :param topic: string with the Kafka topic name to subscribe.
        :param topic_options: additional options for consuming from topic. See docs:
        https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html.
        :param stream: flag to indicate the reading mode: stream or batch
        """
        super().__init__(id, spark_client)
        if not isinstance(connection_string, str):
            raise ValueError(
                "connection_string must be a string with hosts and ports to connect"
            )
        if not isinstance(topic, str):
            raise ValueError("topic must be a string with the topic name")
        self.connection_string = connection_string
        self.topic = topic
        self.options = dict(
            {
                "kafka.bootstrap.servers": self.connection_string,
                "subscribe": self.topic,
            },
            **topic_options if topic_options else {},
        )
        self.stream = stream

    def consume(self):
        """Extract data from a kafka topic.

        :return: Spark dataframe
        """
        raw_stream_df = self.client.read(
            format="kafka", options=self.options, stream=self.stream
        )

        # cast key and value columns from binary to string
        return raw_stream_df.withColumn(
            "key", raw_stream_df["key"].cast("string")
        ).withColumn("value", raw_stream_df["value"].cast("string"))
