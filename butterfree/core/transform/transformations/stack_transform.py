"""H3 Transform entity."""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import array, explode

from butterfree.core.transform.transformations.transform_component import (
    TransformComponent,
)


class StackTransform(TransformComponent):
    """Defines a Stack transformation.

    For instantiation needs the name of the columns or a prefix string to use to
    find the columns that need to be stacked. This transform generates just
    one column as output.

    Attributes:
        columns_names: names of the columns.
        columns_prefix: latitude column.

    Raises:
        ValueError: when neither or both of the arguments are different than None.

    Example:
        >>> from pyspark import SparkContext
        >>> from pyspark.sql import session
        >>> from butterfree.testing.dataframe import create_df_from_collection
        >>> from butterfree.core.transform.transformations import StackTransform
        >>> from butterfree.core.transform.features import Feature
        >>> spark_context = SparkContext.getOrCreate()
        >>> spark_session = session.SparkSession(spark_context)
        >>> data = [
        ...    {"feature": 100, "id_a": 1, "id_b": 2},
        ...    {"feature": 120, "id_a": 3, "id_b": 4},
        ... ]
        >>> df = create_df_from_collection(data, spark_context, spark_session)
        >>> df.collect()
        [Row(feature=100, id_a=1, id_b=2), Row(feature=120, id_a=3, id_b=4)]
        >>> feature = Feature(
        ...     name="stack_ids",
        ...     description="id_a and id_b stacked in a single column.",
        ...     transformation=StackTransform(columns_names=["id_a", "id_b"]),
        ... )
        >>> feature.transform(df).collect()
        [
            Row(feature=100, id_a=1, id_b=2, stack_ids=1),
            Row(feature=100, id_a=1, id_b=2, stack_ids=2),
            Row(feature=120, id_a=3, id_b=4, stack_ids=3),
            Row(feature=120, id_a=3, id_b=4, stack_ids=4)
        ]

        The StackTransform can be instantiated using a column prefix instead of
        columns names. Like this way:
        >>> feature = Feature(
        ...     name="stack_ids",
        ...     description="id_a and id_b stacked in a single column.",
        ...     transformation=StackTransform(columns_prefix="id_"),
        ... )

    """

    def __init__(self, columns_names: List[str] = None, columns_prefix: str = None):
        if columns_names is None and columns_prefix is None:
            raise ValueError(
                "At least one of the args columns_names or columns_prefix should be set, both can't be None."
            )

        if columns_names is not None and columns_prefix is not None:
            raise ValueError(
                "Just one of the args columns_names or columns_prefix should be set, not both."
            )

        super().__init__()
        self.columns_names = columns_names
        self.columns_prefix = columns_prefix

    @property
    def output_columns(self) -> List[str]:
        """Columns generated by the transformation."""
        return [self._parent.name]

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe.

        Returns:
            Transformed dataframe.

        """
        if self.columns_names:
            columns = self.columns_names
            if not all(c in dataframe.columns for c in columns):
                raise ValueError(
                    "Not all columns found, columns in df: {}, target columns: {}".format(
                        dataframe.columns, columns
                    )
                )

        if self.columns_prefix:
            columns = [
                column
                for column in dataframe.columns
                if column.startswith(self.columns_prefix)
            ]
            if not columns:
                raise ValueError(
                    "Columns not found, columns in df: {}, target columns prefix: '{}'".format(
                        dataframe.columns, self.columns_prefix
                    )
                )

        return dataframe.withColumn(self._parent.name, explode(array(*columns)))
