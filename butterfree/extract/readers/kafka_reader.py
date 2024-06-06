"""KafkaSource entity."""

from typing import Any, Dict

from pyspark.sql.dataframe import DataFrame, StructType
from pyspark.sql.functions import col, struct

from butterfree.clients import SparkClient
from butterfree.configs import environment
from butterfree.extract.pre_processing import explode_json_column
from butterfree.extract.readers.reader import Reader


class KafkaReader(Reader):
    """Responsible for get data from a Kafka topic.

    Attributes:
        id: unique string id for register the reader as a view on the metastore
        value_schema: expected schema of the default column named "value" from Kafka.
        topic: string with the Kafka topic name to subscribe.
        connection_string: string with hosts and ports to connect.
            The string need to be in the format: host1:port,host2:port,...,hostN:portN.
            The argument is not necessary if is passed as a environment variable
            named KAFKA_CONSUMER_CONNECTION_STRING.
        topic_options: additional options for consuming from topic. See docs:
            https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html.
        stream: flag to indicate the reading mode: stream or batch

    The default df schema coming from Kafka reader of Spark is the following:
        key:string
        value:string
        topic:string
        partition:integer
        offset:long
        timestamp:timestamp
        timestampType:integer

    But using this reader and passing the desired schema under value_schema we would
    have the following result:

    With value_schema declared as:

    >>> value_schema = StructType(
    ...     [
    ...         StructField("ts", LongType(), nullable=True),
    ...         StructField("id", LongType(), nullable=True),
    ...         StructField("type", StringType(), nullable=True),
    ...     ]
    ... )

    The output df schema would be:
        ts:long
        id:long
        type:string
        kafka_metadata:struct
            key:string
            topic:string
            value:string
            partition:integer
            offset:long
            timestamp:timestamp
            timestampType:integer

    Instantiation example:

    >>> from butterfree.extract.readers import KafkaReader
    >>> from butterfree.clients import SparkClient
    >>> from pyspark.sql.types import StructType, StructField, StringType, LongType
    >>> spark_client = SparkClient()
    >>> value_schema = StructType(
    ...     [
    ...         StructField("ts", LongType(), nullable=True),
    ...         StructField("id", LongType(), nullable=True),
    ...         StructField("type", StringType(), nullable=True),
    ...     ]
    ... )
    >>> kafka_reader = KafkaReader(
    ...                 id="kafka_reader_id",
    ...                 topic="topic",
    ...                 value_schema=value_schema
    ...                 connection_string="host1:port,host2:port",
    ...                )
    >>> df = kafka_reader.consume(spark_client)

    This last method will use the Spark Client, as default, to read
    the desired topic, loading data into a dataframe, according to
    KafkaReader class arguments.

    In this case, however, a temp view will be created, containing
    the transformed data.

    """

    KAFKA_COLUMNS = [
        "key",
        "topic",
        "value",
        "partition",
        "offset",
        "timestamp",
        "timestampType",
    ]

    __name__ = "Kafka Reader"

    def __init__(
        self,
        id: str,
        topic: str,
        value_schema: StructType,
        connection_string: str = None,
        topic_options: Dict[Any, Any] = None,
        stream: bool = True,
    ):
        super().__init__(id)
        if not isinstance(topic, str):
            raise ValueError("topic must be a string with the topic name")
        if not isinstance(value_schema, StructType):
            raise ValueError(
                "value_schema must be a StructType with the schema "
                'of the JSON presented in "value" Kafka column'
            )
        self.topic = topic
        self.value_schema = value_schema
        self.connection_string = connection_string or environment.get_variable(
            "KAFKA_CONSUMER_CONNECTION_STRING"
        )
        self.options = dict(
            {
                "kafka.bootstrap.servers": self.connection_string,
                "subscribe": self.topic,
            },
            **topic_options if topic_options else {},
        )
        self.stream = stream

    def _struct_df(self, df: DataFrame) -> DataFrame:
        """Struct the output dataframe generated by the reader.

        Under the default "value" column coming from Kafka there are the custom
        fields created by some producer. This function will struct the dataframe as
        to get all desired fields from "value" and insert all Kafka default columns,
        including "value", under "kafka_metadata" column. It is important to notice
        that the declared value_schema suffer from the same effects described in
        explode_json_column method in pre_processing module.

        Args:
            df: direct dataframe output from from KafkaReader.

        Returns:
            Structured dataframe with kafka value fields as columns.
                All other default fields from Kafka will be stored under
                "kafka_metadata" column.

        """
        df = df.withColumn("kafka_metadata", struct(*self.KAFKA_COLUMNS))
        df = explode_json_column(df, column="value", json_schema=self.value_schema)
        return df.select(
            [field.name for field in self.value_schema] + ["kafka_metadata"]
        )

    def consume(self, client: SparkClient) -> DataFrame:
        """Extract data from a kafka topic.

        When stream mode it will get all the new data arriving at the topic in a
        streaming dataframe. When not in stream mode it will get all data
        available in the kafka topic.

        Args:
            client: client responsible for connecting to Spark session.

        Returns:
            Dataframe with data from topic.

        """
        # read using client and cast key and value columns from binary to string
        raw_df = (
            client.read(format="kafka", stream=self.stream, **self.options)
            .withColumn("key", col("key").cast("string"))
            .withColumn("value", col("value").cast("string"))
        )

        # apply schema defined in self.value_schema
        return self._struct_df(raw_df)
