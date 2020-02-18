"""DataType Enum Entity."""

from enum import Enum

from pyspark.sql.types import TimestampType


class DataType(Enum):
    """Holds constants for data types within Butterfree."""

    TIMESTAMP = TimestampType()
