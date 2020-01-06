from pyspark.sql import SparkSession

from quintoandar_butterfree.core.input.kafka import KafkaConsumer
from quintoandar_butterfree.core.input import Input
from quintoandar_butterfree.mappings import ListingPageViewedMapping


class Kafka(Input):
    _MAPPINGS = {"listing_page_viewed": ListingPageViewedMapping()}

    def __init__(self, event, spark: SparkSession = None):
        self._consumer = KafkaConsumer(self._MAPPINGS[event])
        self._spark = spark or SparkSession.builder.getOrCreate()

    def fetch(self):
        return self._consumer.listen(self._spark)
