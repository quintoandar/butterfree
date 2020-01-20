"""Aggregated Transform entity."""

from typing import Dict, List

from parameters_validation import non_blank
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from butterfree.core.transform.aggregation.window_mapping import WindowType
from butterfree.core.transform.transform_component import TransformComponent


class Aggregation(TransformComponent):
    """Defines an Aggregation.

    Attributes:
        aggregations: list containing the desired aggregations.
        windows: time windows.
        partition: partition definition.
        time_column: time column definition.
    """

    def __init__(
        self,
        aggregations: non_blank(List[str]),
        windows: non_blank(Dict),
        partition: non_blank(str) = None,
        time_column: str = None,
    ):
        super().__init__()
        self._aggregations = aggregations
        self._windows = windows
        self._partition = partition
        self._parent = None
        self._time_column = time_column or "timestamp"

    def _get_alias(self, alias):
        if alias is not None:
            return self._parent.alias
        else:
            return self._parent.name

    @staticmethod
    def _get_agg_method(aggregation, feature_name, w):
        if aggregation in ["avg"]:
            return F.avg(feature_name).over(w)
        elif aggregation in ["std"]:
            return F.stddev_pop(feature_name).over(w)
        else:
            raise ValueError()

    def transform(self, dataframe: DataFrame):
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: base dataframe.

        Returns:
            dataframe: transformed dataframe.
        """
        for aggregation in self._aggregations:
            for window_type, window_lenght in self._windows.items():
                name = self._get_alias(self._parent.alias)
                feature_name = (
                    f"{name}__{aggregation}_over_{str(window_lenght)}_{window_type}"
                )
                w = (
                    Window()
                    .partitionBy(F.col(f"{self._partition}"))
                    .orderBy(F.col(f"{self._time_column}").cast("long"))
                    .rangeBetween(
                        -(WindowType.convert_to_seconds(window_type, window_lenght)), 0
                    )
                )

                dataframe = dataframe.select(F.col("*")).withColumn(
                    feature_name,
                    self._get_agg_method(aggregation, f"{self._parent.name}", w),
                )

        return dataframe
