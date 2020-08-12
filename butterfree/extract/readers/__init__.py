"""The Reader Component of a Source."""
from butterfree.extract.readers.file_reader import FileReader
from butterfree.extract.readers.kafka_reader import KafkaReader
from butterfree.extract.readers.table_reader import TableReader

__all__ = ["FileReader", "KafkaReader", "TableReader"]
