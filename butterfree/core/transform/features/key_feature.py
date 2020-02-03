"""KeyFeature entity."""

from butterfree.core.constant.data_type import DataType
from butterfree.core.transform.features.feature import Feature
from butterfree.core.transform.transformations import TransformComponent


class KeyFeature(Feature):
    """Defines a KeyFeature.

    A FeatureSet must contain one or more KeyFeatures, which will be used as keys when
    storing the feature set dataframe as tables. The Feature Set may validate keys are
    unique for the latest state of a feature set.

    Attributes:
        name: key name.
            Can be use by the transformation to derive multiple key columns.
        description: brief explanation regarding the key.
        dtype: data type for the output columns of this key.
        from_column: original column to build a key.
            Used when there is transformation or the transformation has no reference
            about the column to use for.
        transformation: transformation that will be applied to create this key.
            Keys can be derived by transformations over any data column. Like a location
            hash based on latitude and longitude.
    """

    def __init__(
        self,
        name: str,
        description: str,
        dtype: DataType = None,
        from_column: str = None,
        transformation: TransformComponent = None,
    ) -> None:
        super(KeyFeature, self).__init__(
            name=name,
            description=description,
            dtype=dtype,
            from_column=from_column,
            transformation=transformation,
        )