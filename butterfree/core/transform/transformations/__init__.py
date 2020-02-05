"""Holds all transformations to be used by Features.

A transformation must inherit from a TransformComponent and handle data modification,
renaming and cast types using parent's (a Feature) information.
"""

from butterfree.core.transform.transformations.aggregated_transform import (
    AggregatedTransform,
)
from butterfree.core.transform.transformations.custom_transform import CustomTransform
from butterfree.core.transform.transformations.h3_transform import H3HashTransform
from butterfree.core.transform.transformations.transform_component import (
    TransformComponent,
)

__all__ = [
    "AggregatedTransform",
    "CustomTransform",
    "H3HashTransform",
    "TransformComponent",
]
