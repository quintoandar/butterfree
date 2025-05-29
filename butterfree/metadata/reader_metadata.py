from typing import Literal

from pydantic import BaseModel, Field


class FileReaderMetadata(BaseModel):
    """Metadata model for File Reader.

    Attributes:
        path: path to the file or directory.
        format: file format (e.g., parquet, csv, json).
    """

    type: Literal["FileReader"] = Field("FileReader", frozen=True)
    incremental_strategy: bool = Field(
        ..., description="Whether the reader has an incremental strategy"
    )
    path: str = Field(..., description="Path to the file or directory")
    format: str = Field(..., description="File format (e.g., parquet, csv, json)")


class KafkaReaderMetadata(BaseModel):
    """Metadata model for Kafka Reader.

    Attributes:
        topic: Kafka topic to read from.
    """

    type: Literal["KafkaReader"] = Field("KafkaReader", frozen=True)
    topic: str = Field(..., description="Kafka topic to read from")


class TableReaderMetadata(BaseModel):
    """Metadata model for Table Reader.

    Attributes:
        database: database name.
        table: table name.
    """

    type: Literal["TableReader"] = Field("TableReader", frozen=True)
    incremental_strategy: bool = Field(
        ..., description="Whether the reader has an incremental strategy"
    )
    database: str = Field(..., description="Database name")
    table: str = Field(..., description="Table name")
