"""TimestampFeature entity."""

from butterfree.core.constant.data_type import DataType
from butterfree.core.transform.features.feature import Feature
from butterfree.core.transform.transformations import TransformComponent


class TimestampFeature(Feature):
    """Defines a TimestampFeature.

    A FeatureSet must contain one TimestampFeature, which will be used as a time tag
    for the state of all features. By containing a timestamp feature, users may time
    travel over their features. The Feature Set may validate that the set of keys and
    timestamp are unique for a feature set.

    By defining a TimestampColumn, the feature set will always contain a data column
    called "timestamp" of TimestampType (spark dtype).

    Attributes:
        from_column: original column to build a "timestamp" feature column.
            Used when there is transformation or the transformation has no reference
            about the column to use for.
            If from_column is None, the FeatureSet will assume the input dataframe
            already has a data column called "timestamp".
        transformation: transformation that will be applied to create the "timestamp".
            Type casting will already happen when no transformation is given. But a
            timestamp can be derived from multiple columns, like year, month and day,
            for example. The transformation must always handle naming and typing.
    """

    def __init__(
        self, from_column: str = None, transformation: TransformComponent = None
    ):
        description = "Records when any data for this feature set."
        super(TimestampFeature, self).__init__(
            name="timestamp",
            description=description,
            from_column=from_column,
            dtype=DataType.TIMESTAMP,
            transformation=transformation,
        )
