"""Reader metadata entities.

Created here to avoid circular imports since the readers are imported in the
pipelines module (__init__.py).
"""

from pydantic import BaseModel, Field


class BaseReaderMetadata(BaseModel):
    """Base metadata model for Readers.

    Attributes:
        id: unique string id for register the reader as a view on the metastore.
        type: type of the reader (file, kafka, or table).
        incremental_strategy: whether the reader has an incremental strategy.
        stream: whether the reader is streaming data.
    """

    id: str = Field(
        ...,
        description="Unique string id for register the reader as a view",
    )
    type: str = Field(..., description="Type of the reader (file, kafka, or table)")
    incremental_strategy: bool = Field(
        ..., description="Whether the reader has an incremental strategy"
    )
    stream: bool = Field(..., description="Whether the reader is streaming data")


class FileReaderMetadata(BaseReaderMetadata):
    """Metadata model for File Reader.

    Attributes:
        path: path to the file or directory.
        format: file format (e.g., parquet, csv, json).
    """

    path: str = Field(..., description="Path to the file or directory")
    format: str = Field(..., description="File format (e.g., parquet, csv, json)")


class KafkaReaderMetadata(BaseReaderMetadata):
    """Metadata model for Kafka Reader.

    Attributes:
        topic: Kafka topic to read from.
    """

    topic: str = Field(..., description="Kafka topic to read from")


class TableReaderMetadata(BaseReaderMetadata):
    """Metadata model for Table Reader.

    Attributes:
        database: database name.
        table: table name.
    """

    database: str = Field(..., description="Database name")
    table: str = Field(..., description="Table name")
