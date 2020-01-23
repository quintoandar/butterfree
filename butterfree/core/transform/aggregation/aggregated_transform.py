"""Aggregated Transform entity."""

from typing import Dict, List

from parameters_validation import non_blank
from pyspark.sql import DataFrame, functions
from pyspark.sql.window import Window

from butterfree.core.transform.transform_component import TransformComponent


class AggregatedTransform(TransformComponent):
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
        windows: non_blank(Dict[str, List[int]]),
        partition: non_blank(str) = None,
        time_column: str = None,
    ):
        super().__init__()
        self.aggregations = aggregations
        self.windows = windows
        self.partition = partition
        self.parent = None
        self.time_column = time_column or "timestamp"

    __ALLOWED_AGGREGATIONS = {"avg": functions.avg, "std": functions.stddev_pop}
    __ALLOWED_WINDOWS = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
        "days": 86400,
        "weeks": 604800,
        "months": 2419200,
        "years": 29030400,
    }

    @property
    def aggregations(self):
        """Returns the aggregations."""
        return self._aggregations

    @aggregations.setter
    def aggregations(self, value: List[str]):
        aggregations = []
        if not value:
            raise ValueError("Aggregations must not be empty.")
        for agg in value:
            if agg not in self.allowed_aggregations:
                raise KeyError(
                    f"{agg} is not supported. These are the allowed "
                    f"aggregations that you can use: "
                    f"{self.allowed_aggregations}"
                )
            aggregations.append(agg)
        self._aggregations = aggregations

    @property
    def allowed_aggregations(self):
        """Returns list of allowed aggregations."""
        return list(self.__ALLOWED_AGGREGATIONS.keys())

    @property
    def windows(self):
        """Returns the windows."""
        return self._windows

    @windows.setter
    def windows(self, windows: Dict[str, List[int]]):
        if not windows:
            raise KeyError("Windows must not be empty.")

        for window_unit, window_sizes in windows.items():
            if window_unit not in self.allowed_windows:
                raise KeyError(
                    f"{window_unit} is not supported. These are the allowed "
                    f"time windows that you can use: "
                    f"{self.allowed_windows}"
                )
            if not isinstance(window_sizes, List):
                raise KeyError(f"Windows must be a list.")
            if len(window_sizes) == 0:
                raise KeyError(f"Windows must have one item at least.")
            if not all(window_size >= 0 for window_size in window_sizes):
                raise KeyError(f"{window_sizes} have negative element.")
        self._windows = windows

    @property
    def allowed_windows(self):
        """Returns list of allowed windows."""
        return list(self.__ALLOWED_WINDOWS.keys())

    @staticmethod
    def _window_definition(partition: str, time_column: str, window_span: int):
        w = (
            Window()
            .partitionBy(functions.col(partition))
            .orderBy(functions.col(time_column).cast("long"))
            .rangeBetween(-window_span, 0)
        )

        return w

    def transform(self, dataframe: DataFrame):
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: base dataframe.

        Returns:
            dataframe: transformed dataframe.
        """
        for aggregation in self._aggregations:
            for window_unit, window_sizes in self._windows.items():
                for window_size in window_sizes:
                    name = self._get_alias(self._parent.alias)
                    feature_name = (
                        f"{name}__{aggregation}_over_{str(window_size)}_{window_unit}"
                    )
                    w = self._window_definition(
                        partition=f"{self.partition}",
                        time_column=f"{self.time_column}",
                        window_span=self.__ALLOWED_WINDOWS[window_unit] * window_size,
                    )

                    dataframe = dataframe.select(functions.col("*")).withColumn(
                        feature_name,
                        self.__ALLOWED_AGGREGATIONS[aggregation](
                            f"{self._parent.name}"
                        ).over(w),
                    )

        return dataframe
