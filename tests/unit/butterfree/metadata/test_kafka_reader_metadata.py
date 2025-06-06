from pyspark.sql.types import LongType, StructField, StructType

from butterfree.extract.readers import KafkaReader
from butterfree.metadata.reader_metadata import KafkaReaderMetadata


class TestKafkaReaderMetadata:
    def test_build_metadata(self):
        # given
        value_schema = StructType([StructField("id", LongType())])
        kafka_reader = KafkaReader(
            id="kafka_reader",
            topic="topic",
            value_schema=value_schema,
        )

        # when
        metadata = kafka_reader.build_metadata()

        # then
        assert isinstance(metadata, KafkaReaderMetadata)
        assert metadata.topic == kafka_reader.topic
