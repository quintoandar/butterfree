"""DataType Enum Entity."""

from enum import Enum

from pyspark.sql.types import TimestampType, BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType


class DataType(Enum):
    """Holds constants for data types within Butterfree."""

    TIMESTAMP = TimestampType()
    BINARY = BinaryType()
    BOOLEAN = BooleanType()
    BYTE = ByteType()
    DATE = DateType()
    DECIMAL = DecimalType()
    DOUBLE = DoubleType()
    FLOAT = FloatType()
    INTEGER = IntegerType()
    BIGINT = LongType()
    SMALLINT = ShortType()
    STRING = StringType()
