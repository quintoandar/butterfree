from enum import Enum


class FeatureType(Enum):
    """
    Enum Class defining the data types for features in our databases.
    Currently defines the type used by cassandra.
    """

    TEXT = "text"
    INT = "int"
    BIGINT = "bigint"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    UUID = "uuid"
    FLOAT = "float"
    FROZEN_LIST_BIGINT = "frozen<list<bigint>>"
    FROZEN_LIST_TEXT = "frozen<list<text>>"
    TIMESTAMP = "timestamp"

    def __init__(self, cassandra_mapping):
        self.cassandra_mapping = cassandra_mapping
