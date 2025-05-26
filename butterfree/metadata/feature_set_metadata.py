from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field

from butterfree.metadata.feature_metadata import FeatureMetadata


class FeatureSetMetadata(BaseModel):
    """Metadata model for a feature set catalog.

    This model represents the catalog information of a feature set,
    including its name, description, and column definitions.
    """

    feature_set_name: str = Field(..., description="The name of the Feature Set")
    description: str = Field(..., description="The description of the Feature Set")
    columns: List[FeatureMetadata] = Field(
        ..., description="A list of column definitions"
    )
