"""Holds all transformations to be used by Features.

A transformation must inherit from a TransformComponent and handle data modification,
renaming and cast types using parent's (a Feature) information.
"""

from butterfree.transform.transformations.user_defined_functions.mode import mode
from butterfree.transform.transformations.user_defined_functions.most_frequent_set import (  # noqa
    most_frequent_set,
)

__all__ = [
    "mode",
    "most_frequent_set",
]
