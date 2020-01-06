from typing import List, Tuple

from parameters_validation import parameter_validation, validate_parameters, non_blank
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window

from quintoandar_butterfree.core.data_types import FeatureType


@parameter_validation
def non_reserved(feature_name: str):
    reserved_names = ["dt"]
    if feature_name in reserved_names:
        raise ValueError(
            "`{feature_name}` is a reserved partitioning name".format(
                feature_name=feature_name
            )
        )


class Feature:
    @validate_parameters
    def __init__(
        self,
        *,
        column: non_blank(str),
        description: non_blank(str),
        alias: str = None,
        aggregations: List[str] = None,
        group_by: str = None,
        windows: List[str] = None,
        slide_duration: str = None,
        time_column: str = None,
        data_type: FeatureType = None,
        key_column: bool = False,
    ):
        self._column = column
        self._name = alias or self._column
        self._aggregations = aggregations
        self._group_by = group_by
        self._windows = windows or ["1 year"]
        self._slide_duration = slide_duration or "1 day"
        self._time_column = time_column or "timestamp"
        self._description = description
        self._data_type = data_type
        self._key_column = key_column

    def compute(self, dataframe: DataFrame) -> Tuple[DataFrame, List[str]]:
        if not self._aggregations:
            return dataframe.withColumn(self._name, col(self._column)), [self._name]

        computed_features = []
        for aggregation in self._aggregations:
            for window_span in self._windows:
                feature_name = (
                    f"{self._name}__{aggregation}_over_{window_span.replace(' ', '_')}"
                )
                computed_features.append(feature_name)
                dataframe = (
                    dataframe.withWatermark(self._time_column, "10 minutes")
                    .groupBy(
                        self._group_by,
                        window(self._time_column, window_span, self._slide_duration),
                    )
                    .agg({self._column: aggregation})
                    .withColumnRenamed(f"{aggregation}({self._column})", feature_name)
                )

        return dataframe, computed_features
