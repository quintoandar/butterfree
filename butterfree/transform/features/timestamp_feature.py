"""TimestampFeature entity."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp

from butterfree.constants import DataType
from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.transform.features import Feature
from butterfree.transform.transformations import TransformComponent


class TimestampFeature(Feature):
    """Defines a TimestampFeature.

    A FeatureSet must contain one TimestampFeature, which will be used as a time
    tag for the state of all features. By containing a timestamp feature, users
    may time travel over their features. The Feature Set may validate that the
    set of keys and timestamp are unique for a feature set.

    By defining a TimestampColumn, the feature set will always contain a data
    column called "timestamp" of TimestampType (spark dtype).

    Attributes:
        from_column: original column to build a "timestamp" feature column.
            Used when there is transformation or the transformation has no
            reference about the column to use for.
            If from_column is None, the FeatureSet will assume the input
            dataframe already has a data column called "timestamp".
        transformation: transformation that will be applied to create the
            "timestamp". Type casting will already happen when no transformation
            is given. But a timestamp can be derived from multiple columns, like
            year, month and day, for example. The transformation must always
            handle naming and typing.
        from_ms: true if timestamp column presents milliseconds time unit. A
        conversion is then performed.
        mask: specified timestamp format by the user.

    """

    def __init__(
        self,
        from_column: str = None,
        transformation: TransformComponent = None,
        from_ms: bool = False,
        mask: str = None,
    ) -> None:
        description = "Time tag for the state of all features."
        super(TimestampFeature, self).__init__(
            name=TIMESTAMP_COLUMN,
            description=description,
            from_column=from_column,
            dtype=DataType.TIMESTAMP,
            transformation=transformation,
        )
        self.from_ms = from_ms
        self.mask = mask

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe for the transformation.

        Returns:
            Transformed dataframe.
        """
        column_name = self.from_column if self.from_column else self.name

        ts_column = dataframe[column_name]
        if self.from_ms:
            ts_column = ts_column / 1000

        dataframe = dataframe.withColumn(
            column_name, to_timestamp(ts_column, self.mask)
        )

        return super().transform(dataframe)
