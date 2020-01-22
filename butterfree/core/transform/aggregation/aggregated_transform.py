"""Aggregated Transform entity."""

from typing import Dict, List

from parameters_validation import non_blank
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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
        windows: non_blank(Dict),
        partition: non_blank(str) = None,
        time_column: str = None,
    ):
        super().__init__()
        self.aggregations = aggregations
        self.windows = windows
        self.partition = partition
        self.parent = None
        self.time_column = time_column or "timestamp"

    __ALLOWED_AGGREGATIONS = {"avg": F.avg, "std": F.stddev_pop}
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
        self._aggregations = []
        if not value:
            raise IndexError("Aggregations must not be empty.")
        for agg in value:
            try:
                self._aggregations.append(
                    list(self.__ALLOWED_AGGREGATIONS.keys())[
                        list(self.__ALLOWED_AGGREGATIONS.values()).index(
                            self.__ALLOWED_AGGREGATIONS[agg]
                        )
                    ]
                )
            except KeyError as e:
                e.args = (
                    f"{agg} is not supported. These are the allowed "
                    f"aggregations that you can use: "
                    f"{self.allowed_aggregations}"
                )
                raise KeyError(e.args)

    @property
    def allowed_aggregations(self):
        """Returns list of allowed aggregations."""
        return list(self.__ALLOWED_AGGREGATIONS.keys())

    @property
    def windows(self):
        """Returns the windows."""
        return self._windows

    @windows.setter
    def windows(self, windows: Dict):
        self._windows = {}
        if not windows:
            raise KeyError("Windows must not be empty.")
        for window_type, window_lenght in windows.items():
            try:
                self._windows.update(
                    {
                        list(self.__ALLOWED_WINDOWS.keys())[
                            list(self.__ALLOWED_WINDOWS.values()).index(
                                self.__ALLOWED_WINDOWS[window_type]
                            )
                        ]: window_lenght
                    }
                )
            except KeyError as e:
                e.args = (
                    f"{window_type} is not supported. These are the allowed "
                    f"time windows that you can use: "
                    f"{self.allowed_windows}"
                )
                raise KeyError(e.args)

    @property
    def allowed_windows(self):
        """Returns list of allowed windows."""
        return list(self.__ALLOWED_WINDOWS.keys())

    def transform(self, dataframe: DataFrame):
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: base dataframe.

        Returns:
            dataframe: transformed dataframe.
        """
        for aggregation in self._aggregations:
            for window_type, window_lenght in self.windows.items():
                name = self._get_alias(self._parent.alias)
                feature_name = (
                    f"{name}__{aggregation}_over_{str(window_lenght)}_{window_type}"
                )
                w = (
                    Window()
                    .partitionBy(F.col(f"{self.partition}"))
                    .orderBy(F.col(f"{self.time_column}").cast("long"))
                    .rangeBetween(
                        -(self.__ALLOWED_WINDOWS[window_type] * window_lenght), 0
                    )
                )

                dataframe = dataframe.select(F.col("*")).withColumn(
                    feature_name,
                    self.__ALLOWED_AGGREGATIONS[aggregation](
                        f"{self._parent.name}"
                    ).over(w),
                )

        return dataframe
