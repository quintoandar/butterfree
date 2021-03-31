"""DataType Enum Entity."""

from enum import Enum

from pyspark.sql.types import ArrayType, BinaryType, BooleanType
from pyspark.sql.types import DataType as PySparkDataType
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)
from typing_extensions import final


@final
class DataType(Enum):
    """Holds constants for data types within Butterfree."""

    TIMESTAMP = (TimestampType(), "timestamp", "TIMESTAMP")
    BINARY = (BinaryType(), "boolean", "BINARY")
    BOOLEAN = (BooleanType(), "boolean", "BOOLEAN")
    DATE = (DateType(), "timestamp", "DATE")
    DECIMAL = (DecimalType(), "decimal", "DECIMAL")
    DOUBLE = (DoubleType(), "double", "DOUBLE")
    FLOAT = (FloatType(), "float", "FLOAT")
    INTEGER = (IntegerType(), "int", "INT")
    BIGINT = (LongType(), "bigint", "BIGINT")
    STRING = (StringType(), "text", "STRING")
    ARRAY_BIGINT = (ArrayType(LongType()), "frozen<list<bigint>>", "ARRAY<BIGINT>")
    ARRAY_STRING = (ArrayType(StringType()), "frozen<list<text>>", "ARRAY<STRING>")
    ARRAY_FLOAT = (ArrayType(FloatType()), "frozen<list<float>>", "ARRAY<FLOAT>")

    def __init__(self, spark: PySparkDataType, cassandra: str, spark_sql: str) -> None:
        self.spark = spark
        self.cassandra = cassandra
        self.spark_sql = spark_sql
