from enum import Enum

from pyspark.sql.types import TimestampType


class DataType(Enum):
    TIMESTAMP = TimestampType()
