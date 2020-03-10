"""Aggregated Transform entity."""
import warnings
from functools import reduce
from typing import List

from parameters_validation import non_blank
from pyspark.sql import DataFrame, functions
from pyspark.sql.window import Window

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.transformations.transform_component import (
    TransformComponent,
)


class AggregatedTransform(TransformComponent):
    """Defines an Aggregation.

    Attributes:
        mode: available modes to be used in time aggregations, which are
        fixed_windows or rolling_windows.
        aggregations: aggregations to be used in the windows, it can be
            avg, std and count.
        windows: time ranges to be used in the windows, it can be second(s),
            minute(s), hour(s), day(s), week(s), month(s) and year(s).
        partition: column to be used in window partition.
        time_column: timestamp column to be use as sorting reference.

    Example:
        It's necessary to declare the desired aggregation method, (average,
        standard deviation and count are currently supported, as it can be
        seen in __ALLOWED_AGGREGATIONS), the partition column, choose both
        window lenght and time unit and, finally, define the mode.
        >>> from butterfree.core.transform.transformations import AggregatedTransform
        >>> from butterfree.core.transform.features import Feature
        >>> from pyspark import SparkContext
        >>> from pyspark.sql import session
        >>> sc = SparkContext.getOrCreate()
        >>> spark = session.SparkSession(sc)
        >>> df = spark.createDataFrame([(1, "2016-04-11 11:31:11", 200),
        ...                             (1, "2016-04-11 11:44:12", 300),
        ...                             (1, "2016-04-11 11:46:24", 400),
        ...                             (1, "2016-04-11 12:03:21", 500)]
        ...                           ).toDF("id", "timestamp", "feature")
        >>> feature_fixed_windows = Feature(
        ...    name="feature",
        ...    description="aggregated transform with fixed windows usage example",
        ...    transformation=AggregatedTransform(
        ...        aggregations=["avg"],
        ...        partition="id",
        ...        windows=["15 minutes"],
        ...        mode=["fixed_windows"],
        ...    )
        ...)
        >>> feature_fixed_windows.transform(df).orderBy("timestamp").show()
        +--------+-----------------------+-----------------------------+
        |feature | id|          timestamp| feature__avg_over_15_minutes|
        +--------+---+-------------------+-----------------------------+
        |     200|  1|2016-04-11 11:31:11|                        200.0|
        |     300|  1|2016-04-11 11:44:12|                        250.0|
        |     400|  1|2016-04-11 11:46:24|                        350.0|
        |     500|  1|2016-04-11 12:03:21|                        500.0|
        +--------+---+-------------------+-----------------------------+
        >>> feature_rolling_windows = Feature(
        ...    name="feature",
        ...    description="aggregated transform with rolling windows usage example",
        ...    transformation=AggregatedTransform(
        ...        aggregations=["avg"],
        ...        partition="id",
        ...        windows=["1 day"],
        ...        mode=["rolling_windows"],
        ...    )
        ...)
        >>> feature_rolling_windows.transform(df).orderBy("timestamp").show()
        +---+-------------------+---------------------------------------+
        | id|          timestamp|feature__avg_over_1_day_rolling_windows|
        +---+-------------------+---------------------------------------+
        |  1|2016-04-11 21:00:00|                                  350.0|
        +---+-------------------+---------------------------------------+

        It's important to notice that rolling_windows mode affects the
        dataframe granularity and, as it's possible to see, returns only
        columns related to its transformation.

    """

    SLIDE_DURATION = "1 day"

    def __init__(
        self,
        mode: non_blank(str),
        aggregations: non_blank(List[str]),
        windows: non_blank(List[str]),
        partition: non_blank(str) = None,
        time_column: str = None,
    ):
        super().__init__()
        self.mode = mode
        self.aggregations = aggregations
        self.windows = windows
        self.partition = partition
        self.time_column = time_column or TIMESTAMP_COLUMN

    __ALLOWED_AGGREGATIONS = {
        "avg": functions.avg,
        "stddev_pop": functions.stddev_pop,
        "count": functions.count,
        "collect_set": functions.collect_set,
    }
    __ALLOWED_WINDOWS = {
        ("second", "seconds"): 1,
        ("minute", "minutes"): 60,
        ("hour", "hours"): 3600,
        ("day", "days"): 86400,
        ("week", "weeks"): 604800,
        ("month", "months"): 2419200,
        ("year", "years"): 29030400,
    }
    __ALLOWED_MODES = ["fixed_windows", "rolling_windows"]

    @property
    def aggregations(self) -> List[str]:
        """Aggregations to be used in the windows."""
        return self._aggregations

    @aggregations.setter
    def aggregations(self, value: List[str]):
        """Aggregations definitions to be used in the windows."""
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
    def allowed_aggregations(self) -> List[str]:
        """Allowed aggregations to be used in the windows."""
        return list(self.__ALLOWED_AGGREGATIONS.keys())

    @property
    def windows(self) -> List[str]:
        """Time ranges to be used in the windows."""
        return self._windows

    @windows.setter
    def windows(self, windows: List[str]):
        """Time ranges definitions to be used in the windows."""
        if not windows:
            raise KeyError("Windows must not be empty.")
        if not isinstance(windows, List):
            raise KeyError(f"Windows must be a list.")
        if len(windows) == 0:
            raise KeyError(f"Windows must have one item at least.")
        for window in windows:
            if window.split()[1] not in self.allowed_windows:
                raise KeyError(
                    f"{window.split()[1]} is not supported. These are the allowed "
                    f"time windows that you can use: "
                    f"{self.allowed_windows}."
                )
            if int(window.split()[0]) <= 0:
                raise KeyError(f"{window} have negative element.")
            if self.mode[
                0
            ] == "rolling_windows" and self._rolling_windows_allowed_duration(window):
                raise ValueError(
                    "Window duration has to be greater or equal than 1 day"
                    " in rolling_windows mode."
                )
        self._windows = windows

    def _rolling_windows_allowed_duration(self, window):
        """Allowed rolling windows durations regarding the slide duration."""
        for key in self.__ALLOWED_WINDOWS.keys():
            if window.split()[1] in key and (
                self.__ALLOWED_WINDOWS[key] * int(window.split()[0])
                < self.__ALLOWED_WINDOWS[("day", "days")]
            ):
                return True
        return

    @property
    def allowed_windows(self) -> List[str]:
        """Allowed time ranges to be used in the windows."""
        allowed_window_units = []
        for (i, j) in self.__ALLOWED_WINDOWS.keys():
            allowed_window_units.extend([i, j])
        return allowed_window_units

    @property
    def mode(self) -> List[str]:
        """Available modes to be used in the windows."""
        return self._mode

    @mode.setter
    def mode(self, value: List[str]):
        """Modes definitions to be used in the windows."""
        modes = []
        if not value:
            raise ValueError("Modes must not be empty.")
        if len(value) > 1:
            raise NotImplementedError("We currently accept just one mode per feature.")
        for mode in value:
            if mode not in self.allowed_modes:
                raise KeyError(
                    f"{mode} is not supported. These are the allowed "
                    f"modes that you can use: "
                    f"{self.allowed_modes}"
                )
            if mode in ["rolling_windows"]:
                warnings.warn(
                    f"{mode} mode will change the dataset granularity! "
                    f"You cannot perform any other transformation. "
                )
            modes.append(mode)
        self._mode = modes

    @property
    def allowed_modes(self) -> List[str]:
        """Allowed modes to be used in the windows."""
        return self.__ALLOWED_MODES

    def _get_feature_name(self, aggregation, window_unit, window_size):
        """Construct features name based on passed criteria."""
        return (
            f"{self._parent.name}__{aggregation}_over_"
            f"{str(window_size)}_{window_unit}_{self.mode[0]}"
        )

    @property
    def output_columns(self) -> List[str]:
        """Columns generated by the transformation."""
        output_columns = []
        for aggregation in self._aggregations:
            for window in self._windows:
                output_columns.append(
                    self._get_feature_name(
                        aggregation, window.split()[1], window.split()[0]
                    )
                )

        return output_columns

    @staticmethod
    def _window_definition(partition: str, time_column: str, window_span: int):
        """Defines windows based on passed criteria."""
        w = (
            Window()
            .partitionBy(functions.col(partition))
            .orderBy(functions.col(time_column).cast("long"))
            .rangeBetween(-window_span, 0)
        )

        return w

    def _get_window_span(self, window_unit: str, window_size: int):
        """Returns window span."""
        for key in self.__ALLOWED_WINDOWS.keys():
            if window_unit in key:
                return self.__ALLOWED_WINDOWS[key] * window_size

    def _fixed_windows_agg(
        self, dataframe: DataFrame, window, feature_name, aggregation
    ):
        """Returns aggregations for fixed_windows mode."""
        w = self._window_definition(
            partition=f"{self.partition}",
            time_column=f"{self.time_column}",
            window_span=self._get_window_span(
                window_unit=window.split()[1], window_size=int(window.split()[0]),
            ),
        )

        dataframe = dataframe.withColumn(
            feature_name,
            self.__ALLOWED_AGGREGATIONS[aggregation](f"{self._parent.name}").over(w),
        )

        if self._parent.dtype:
            dataframe = dataframe.withColumn(
                feature_name, functions.col(feature_name).cast(self._parent.dtype),
            )

        return dataframe

    def _dataframe_list_join(self, df_base, df):
        """Joins a list of passed dataframes base on partition and time columns."""
        return df_base.join(
            df, on=[f"{self.partition}", f"{self.time_column}"], how="full_outer"
        )

    def _rolling_windows_agg(
        self, dataframe: DataFrame, window, aggregation, feature_name, df_list
    ):
        """Returns aggregations for rolling_windows mode."""
        df = (
            dataframe.groupBy(
                f"{self.partition}",
                functions.window(
                    timeColumn=f"{self.time_column}",
                    windowDuration=f"{window.split()[0]} {window.split()[1]}",
                    slideDuration=self.SLIDE_DURATION,
                ),
            ).agg(self.__ALLOWED_AGGREGATIONS[aggregation](f"{self._parent.name}"),)
        ).select(
            functions.col(f"{self.partition}"),
            functions.col(f"{aggregation}({self._parent.name})").alias(feature_name),
            functions.col("window.end").alias(self.time_column),
        )

        df_list.append(df)

        return df_list

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe.

        Returns:
            Transformed dataframe.

        """
        df_list = []
        for aggregation in self._aggregations:
            for window in self._windows:
                feature_name = self._get_feature_name(
                    aggregation=aggregation,
                    window_unit=window.split()[1],
                    window_size=window.split()[0],
                )
                if self.mode[0] in ["fixed_windows"]:
                    dataframe = self._fixed_windows_agg(
                        dataframe=dataframe,
                        window=window,
                        feature_name=feature_name,
                        aggregation=aggregation,
                    )
                elif self.mode[0] in ["rolling_windows"]:
                    df_list = self._rolling_windows_agg(
                        dataframe=dataframe,
                        window=window,
                        feature_name=feature_name,
                        aggregation=aggregation,
                        df_list=df_list,
                    )

        if df_list:
            dataframe = reduce(self._dataframe_list_join, df_list)

        return dataframe
