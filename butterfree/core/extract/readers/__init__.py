"""The Reader Component of a Source."""
from butterfree.core.extract.readers.file_reader import FileReader
from butterfree.core.extract.readers.kafka_reader import KafkaReader
from butterfree.core.extract.readers.table_reader import TableReader

__all__ = ["FileReader", "KafkaReader", "TableReader"]
