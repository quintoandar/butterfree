"""H3 Transform entity."""

from typing import List

from h3 import h3
from parameters_validation import non_blank
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType

from butterfree.core.constant.h3_resolutions import H3Resolution
from butterfree.core.transform.transformations.transform_component import (
    TransformComponent,
)


@udf(StringType())
def define_h3(lat, lng, resolution):
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


class H3Transform(TransformComponent):
    """Defines an Aggregation.

    Attributes:
        h3_resolutions: h3 resolutions from 6 to 12.
        lat_column: latitude column.
        lng_column: longitude column.

    """

    def __init__(
        self, lat_column: non_blank(str), lng_column: non_blank(str),
    ):
        super().__init__()
        self.h3_resolutions = H3Resolution
        self.lat_column = lat_column
        self.lng_column = lng_column

    @property
    def output_columns(self) -> List[str]:
        """Columns generated by the transformation."""
        output_columns = []
        for h3_resolution in self.h3_resolutions:
            output_columns.append(f"{self._parent.name}__{h3_resolution.name}")

        return output_columns

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe.

        Returns:
            Transformed dataframe.

        """
        for h3_resolution in self.h3_resolutions:
            resolution = h3_resolution.value
            dataframe = dataframe.withColumn(
                f"{self._parent.name}__{h3_resolution.name}",
                define_h3(self.lat_column, self.lng_column, lit(resolution)),
            )
        return dataframe
