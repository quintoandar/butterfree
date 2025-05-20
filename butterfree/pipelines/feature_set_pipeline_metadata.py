from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from butterfree.constants import DataType
from butterfree.extract.readers.reader import BaseReaderMetadata
from butterfree.load.writers.writer import BaseWriterMetadata
from butterfree.pipelines import FeatureSetPipeline
from butterfree.transform.aggregated_feature_set import AggregatedFeatureSet


class Column(BaseModel):
    """Metadata model for a column in a feature set.

    This model represents the metadata of a single column in a feature set,
    including its name, data type, and whether it's a primary key.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(..., description="The name of the column")
    data_type: DataType = Field(
        ..., description="The data type of the column (e.g., string, integer)"
    )
    primary_key: bool = Field(
        ...,
        description="Whether the column is a primary (or partition if it's a Cassandra table) key",  # noqa: E501
    )


class Catalog(BaseModel):
    """Metadata model for a feature set catalog.

    This model represents the catalog information of a feature set,
    including its name, description, and column definitions.
    """

    name: str = Field(..., description="The name of the Feature Set")
    description: str = Field(..., description="The description of the Feature Set")
    columns: List[Column] = Field(..., description="A list of column definitions")


class Metadata(BaseModel):
    """Metadata model for a feature set pipeline.

    This model represents the complete metadata of a feature set pipeline,
    including its configuration, data sources, output schema, and processing details.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    feature_set_pipeline: FeatureSetPipeline = Field(
        ..., description="The feature set pipeline to create metadata from"
    )
    is_incremental: bool = Field(
        ..., description="The strategy for data generation (incremental or batch)"
    )
    readers: List[BaseReaderMetadata] = Field(
        ..., description="A list of data sources required to generate the feature set"
    )
    catalog: Catalog = Field(..., description="Metadata about the feature set's output")
    entity: str = Field(
        ..., description="The entity type associated with the feature set"
    )
    key_features: List[Column] = Field(
        ..., description="The key features of the feature set"
    )
    writers: List[BaseWriterMetadata] = Field(
        ..., description="The writers to be used for the feature set"
    )

    feature_set_type: Literal["FeatureSet", "AggregatedFeatureSet"] = Field(
        ..., description="The type of feature set"
    )
    windows_definition: Optional[List[str]] = Field(
        None, description="The definition of the windows for the feature set"
    )

    @classmethod
    def _get_windows_definition(
        cls, feature_set_pipeline: FeatureSetPipeline
    ) -> Optional[List[str]]:
        """Get windows definition if the feature set is an AggregatedFeatureSet.

        Args:
            feature_set_pipeline: The FeatureSetPipeline to get windows from.

        Returns:
            Optional list of window definitions or None if not applicable.
        """
        if (
            isinstance(feature_set_pipeline.feature_set, AggregatedFeatureSet)
            and feature_set_pipeline.feature_set._windows
        ):
            return [
                window.frame_boundaries.window_definition
                for window in feature_set_pipeline.feature_set._windows
            ]
        return None

    @classmethod
    def _create_catalog(cls, feature_set_pipeline: FeatureSetPipeline) -> Catalog:
        """Create a Catalog object from the feature set pipeline.

        Args:
            feature_set_pipeline: The FeatureSetPipeline to create catalog from.

        Returns:
            A Catalog instance with feature set metadata.
        """
        return Catalog(
            name=feature_set_pipeline.feature_set.name,
            description=feature_set_pipeline.feature_set.description,
            columns=[
                Column(
                    name=feature.name,
                    data_type=feature.dtype,
                    primary_key=(
                        True if feature.__class__.__name__ == "KeyFeature" else False
                    ),
                )
                for feature in feature_set_pipeline.feature_set.features
            ],
        )

    @classmethod
    def _is_incremental(cls, feature_set_pipeline: FeatureSetPipeline) -> bool:
        has_incremental_reader = any(
            [
                reader.incremental_strategy
                for reader in feature_set_pipeline.source.readers
            ]
        )

        return has_incremental_reader

    @classmethod
    def _get_key_features(
        cls, feature_set_pipeline: FeatureSetPipeline
    ) -> List[Column]:
        """Get the key features of the feature set.

        Args:
            feature_set_pipeline: The FeatureSetPipeline to get key features from.

        Returns:
            A list of Column instances representing the key features.
        """
        return [
            Column(
                name=key.name,
                data_type=key.dtype,
                primary_key=True,
            )
            for key in feature_set_pipeline.feature_set.keys
        ]

    @classmethod
    def from_pipeline(cls, feature_set_pipeline: FeatureSetPipeline) -> "Metadata":
        """Create a FeatureSetPipelineMetadata from a FeatureSetPipeline.

        Args:
            feature_set_pipeline: The FeatureSetPipeline to create metadata from.

        Returns:
            A FeatureSetPipelineMetadata instance.
        """
        feature_set_type = type(feature_set_pipeline.feature_set).__name__
        is_incremental = cls._is_incremental(feature_set_pipeline)
        windows_definition = cls._get_windows_definition(feature_set_pipeline)
        catalog = cls._create_catalog(feature_set_pipeline)
        key_features = cls._get_key_features(feature_set_pipeline)

        return cls(
            feature_set_pipeline=feature_set_pipeline,
            is_incremental=is_incremental,
            readers=[
                reader.get_metadata() for reader in feature_set_pipeline.source.readers
            ],
            catalog=catalog,
            entity=feature_set_pipeline.feature_set.entity,
            key_features=key_features,
            writers=[
                writer.get_metadata() for writer in feature_set_pipeline.sink.writers
            ],
            feature_set_type=feature_set_type,
            windows_definition=windows_definition,
        )
