from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from butterfree.metadata.feature_metadata import FeatureMetadata


class FeatureSetMetadata(BaseModel):
    """Metadata model for a feature set catalog.

    This model represents the catalog information of a feature set,
    including its name, description, and column definitions.
    """

    entity: str = Field(
        ..., description="The entity type associated with the feature set"
    )
    name: str = Field(..., description="The name of the Feature Set")
    type: Literal["FeatureSet", "AggregatedFeatureSet"] = Field(
        ..., description="The type of feature set"
    )
    description: str = Field(..., description="The description of the Feature Set")
    windows_definition: Optional[List[str]] = Field(
        None, description="The definition of the windows for the feature set"
    )
    features: List[FeatureMetadata] = Field(
        ..., description="A list of column definitions"
    )
