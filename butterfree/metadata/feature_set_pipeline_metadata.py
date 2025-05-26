from __future__ import annotations

from typing import List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from butterfree.load.writers.writer import WriterMetadata
from butterfree.metadata.feature_set_metadata import FeatureSetMetadata
from butterfree.metadata.reader_metadata import (
    FileReaderMetadata,
    KafkaReaderMetadata,
    TableReaderMetadata,
)


class FeatureSetPipelineMetadata(BaseModel):
    """Metadata model for a feature set pipeline.

    This model represents the complete metadata of a feature set pipeline,
    including its configuration, data sources, output schema, and processing details.
    """

    # To accept arbitrary types like FeatureSetPipeline
    model_config = ConfigDict(arbitrary_types_allowed=True)

    entity: str = Field(
        ..., description="The entity type associated with the feature set"
    )
    feature_set_type: Literal["FeatureSet", "AggregatedFeatureSet"] = Field(
        ..., description="The type of feature set"
    )
    windows_definition: Optional[List[str]] = Field(
        None, description="The definition of the windows for the feature set"
    )

    # Required for correct serialization using Union
    readers: List[
        Annotated[
            Union[FileReaderMetadata, KafkaReaderMetadata, TableReaderMetadata],
            Field(discriminator="type"),
        ]
    ] = Field(
        ...,
        description="A list of data sources required to generate the feature set",
    )

    writers: List[WriterMetadata] = Field(
        ..., description="The writers to be used for the feature set"
    )
    catalog: FeatureSetMetadata = Field(
        ..., description="Metadata about the feature set's output"
    )
