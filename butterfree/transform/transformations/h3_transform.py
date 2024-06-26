"""H3 Transform entity."""

from typing import Any, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType

from butterfree.transform.transformations.stack_transform import StackTransform
from butterfree.transform.transformations.transform_component import TransformComponent

try:
    from h3 import h3
except ModuleNotFoundError as e:
    e.msg = (
        "H3 not found. To be able to use this module,"
        "you must install butterfree[h3]."
    )
    raise


@udf(StringType())
def define_h3(lat: float, lng: float, resolution: int) -> Any:
    """UDF for h3 hash retrieval.

    Attributes:
        lat: latitude column.
        lng: longitude column.
        resolution: desired h3 resolution.
    """
    if lat and lng:
        h3_feature = h3.geo_to_h3(lat, lng, resolution)
    else:
        h3_feature = None
    return h3_feature


class H3HashTransform(TransformComponent):
    """Defines a H3 hash transformation.

    Attributes:
        h3_resolutions: h3 resolutions from 6 to 12.
        lat_column: latitude column.
        lng_column: longitude column.

    Example:
        It's necessary to declare the desired h3 resolutions and
        latitude and longitude columns.

        >>> from butterfree.transform.features import Feature
        >>> from butterfree.transform.transformations.h3_transform import (
        ... H3HashTransform
        ...)
        >>> from butterfree.constants import DataType
        >>> from pyspark import SparkContext
        >>> from pyspark.sql import session
        >>> sc = SparkContext.getOrCreate()
        >>> spark = session.SparkSession(sc)
        >>> df = spark.createDataFrame([(1, 200, -23.554190, -46.670723),
        ...                             (1, 300, -23.554190, -46.670723),
        ...                             (1, 400, -23.554190, -46.670723),
        ...                             (1, 500, -23.554190, -46.670723)]
        ...                           ).toDF("id", "feature", "lat", "lng")
        >>> feature = Feature(
        ...    name="feature",
        ...    description="h3 hash transform usage example",
        ...    dtype=DataType.STRING,
        ...    transformation=H3HashTransform(
        ...        h3_resolutions=[6, 7, 8, 9, 10, 11, 12],
        ...        lat_column="lat",
        ...        lng_column="lng",
        ...    )
        ... )
        >>> feature.transform(df).show()
        +-------+---+---------+----------+-------------------+-------------------+
        |feature| id|      lat|       lng|lat_lng__h3_hash__6|lat_lng__h3_hash__7|
        +-------+---+---------+----------+-------------------+-------------------+
        |    200|  1|-23.55419|-46.670723|    86a8100efffffff|    87a8100eaffffff|
        |    300|  1|-23.55419|-46.670723|    86a8100efffffff|    87a8100eaffffff|
        |    400|  1|-23.55419|-46.670723|    86a8100efffffff|    87a8100eaffffff|
        |    500|  1|-23.55419|-46.670723|    86a8100efffffff|    87a8100eaffffff|
        +-------+---+---------+----------+-------------------+-------------------+

    """

    def __init__(
        self,
        h3_resolutions: List[int],
        lat_column: str,
        lng_column: str,
    ):
        super().__init__()
        self.h3_resolutions = h3_resolutions
        self.lat_column = lat_column
        self.lng_column = lng_column
        self.stack_transform: Optional[StackTransform] = None

    @property
    def output_columns(self) -> List[str]:
        """Columns generated by the transformation."""
        output_columns = []
        for h3_resolution in self.h3_resolutions:
            output_columns.append(f"lat_lng__h3_hash__{h3_resolution}")

        if self.stack_transform:
            return [self._parent.name]

        return output_columns

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe.

        Returns:
            Transformed dataframe.

        """
        for h3_resolution in self.h3_resolutions:
            dataframe = dataframe.withColumn(
                f"lat_lng__h3_hash__{h3_resolution}",
                define_h3(self.lat_column, self.lng_column, lit(h3_resolution)),
            )
        if self.stack_transform:
            self.stack_transform._parent = self._parent
            return self.stack_transform.transform(dataframe)
        return dataframe

    def with_stack(self) -> "H3HashTransform":
        """Add a final Stack step to the transformation.

        A new column will be created stacking all the the resolution columns
        generated by H3. The name of this column will be the parent name of
        H3Transform, for example, the name of the KeyFeature that has the H3
        as a transformation.

        """
        self.stack_transform = StackTransform(*self.output_columns)
        return self
