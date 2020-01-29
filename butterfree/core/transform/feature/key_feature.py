from butterfree.core.transform.feature.feature import Feature
from butterfree.core.transform.transformations import TransformComponent


class KeyFeature(Feature):
    def __init__(
        self,
        name: str,
        description: str,
        from_column: str = None,
        transformation: TransformComponent = None,
    ):
        super(KeyFeature, self).__init__(
            name=name,
            description=description,
            from_column=from_column,
            transformation=transformation,
        )
