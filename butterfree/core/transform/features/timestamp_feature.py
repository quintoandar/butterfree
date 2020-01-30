from butterfree.core.constant.data_type import DataType
from butterfree.core.transform.features.feature import Feature
from butterfree.core.transform.transformations import TransformComponent


class TimestampFeature(Feature):
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
