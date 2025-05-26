from __future__ import annotations

from pydantic import BaseModel, Field


class FeatureMetadata(BaseModel):
    """Metadata model for a column in a feature set.

    This model represents the metadata of a single column in a feature set,
    including its name, data type, and whether it's a primary key.
    """

    name: str = Field(..., description="The name of the column")
    data_type: str = Field(
        ...,
        description=(
            "The data type of the column (e.g., StringType, IntegerType) represented by pyspark.sql.types"  # noqa: E501
        ),
    )
    description: str = Field(
        ...,
        description="The description of the column",
    )
    primary_key: bool = Field(
        ...,
        description="Whether the column is a primary (or partition if it's a Cassandra table) key",  # noqa: E501
    )
