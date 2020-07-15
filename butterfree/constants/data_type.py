"""DataType Enum Entity."""

from enum import Enum

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


class DataType(Enum):
    """Holds constants for data types within Butterfree."""

    TIMESTAMP = (TimestampType(), "timestamp")
    BINARY = (BinaryType(), "boolean")
    BOOLEAN = (BooleanType(), "boolean")
    DATE = (DateType(), "timestamp")
    DECIMAL = (DecimalType(), "decimal")
    DOUBLE = (DoubleType(), "double")
    FLOAT = (FloatType(), "float")
    INTEGER = (IntegerType(), "int")
    BIGINT = (LongType(), "bigint")
    STRING = (StringType(), "text")
    ARRAY_BIGINT = (ArrayType(LongType()), "frozen<list<bigint>>")
    ARRAY_STRING = (ArrayType(StringType()), "frozen<list<text>>")
    ARRAY_FLOAT = (ArrayType(FloatType()), "frozen<list<float>>")

    def __init__(self, spark, cassandra):
        self.spark = spark
        self.cassandra = cassandra
