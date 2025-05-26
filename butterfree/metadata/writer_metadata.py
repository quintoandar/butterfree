from typing import Literal

from pydantic import BaseModel, Field


class WriterMetadata(BaseModel):
    """Base metadata model for Writer.

    This model represents the base metadata for all writers,
    including common configuration and settings.
    """

    type: Literal[
        "OnlineFeatureStoreWriter",
        "HistoricalFeatureStoreWriter",
    ] = Field(..., description="Type of the writer")
    interval_mode: bool = Field(
        ..., description="Whether the writer operates in interval mode"
    )
    write_to_entity: bool = Field(
        ..., description="Whether the writer writes to an entity table"
    )
    db_config: str = Field(
        ..., description="Name of the database configuration class used"
    )
