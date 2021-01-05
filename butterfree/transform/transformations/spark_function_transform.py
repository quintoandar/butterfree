"""Spark Function Transform entity."""
from typing import Any, List

from pyspark.sql import DataFrame

from butterfree.transform.transformations.transform_component import TransformComponent
from butterfree.transform.utils import Window
from butterfree.transform.utils.function import Function


class SparkFunctionTransform(TransformComponent):
    """Defines an Spark Function.

    Attributes:
        function: namedtuple with spark function and data type.

    Example:
        It's necessary to declare the function method,
        Any spark and user defined functions are supported.

        >>> from butterfree.transform.transformations import SparkFunctionTransform
        >>> from butterfree.constants.columns import TIMESTAMP_COLUMN
        >>> from butterfree.transform.features import Feature
        >>> from butterfree.transform.utils import Function
        >>> from butterfree.constants import DataType
        >>> from pyspark import SparkContext
        >>> from pyspark.sql import session
        >>> from pyspark.sql.types import TimestampType
        >>> from pyspark.sql import functions
        >>> sc = SparkContext.getOrCreate()
        >>> spark = session.SparkSession(sc)
        >>> df = spark.createDataFrame([(1, "2016-04-11 11:31:11", 200),
        ...                             (1, "2016-04-11 11:44:12", 300),
        ...                             (1, "2016-04-11 11:46:24", 400),
        ...                             (1, "2016-04-11 12:03:21", 500)]
        ...                           ).toDF("id", "timestamp", "feature")
        >>> df = df.withColumn("timestamp", df.timestamp.cast(TimestampType()))
        >>> feature = Feature(
        ...    name="feature",
        ...    description="spark function transform",
        ...    transformation=SparkFunctionTransform(
        ...       functions=[Function(functions.cos, DataType.DOUBLE)],)
        ...)
        >>> feature.transform(df).orderBy("timestamp").show()
        +---+-------------------+-------+--------------------+
        | id|          timestamp|feature|        feature__cos|
        +---+-------------------+-------+--------------------+
        |  1|2016-04-11 11:31:11|    200|  0.4871876750070059|
        |  1|2016-04-11 11:44:12|    300|-0.02209661927868...|
        |  1|2016-04-11 11:46:24|    400|  -0.525296338642536|
        |  1|2016-04-11 12:03:21|    500|  -0.883849273431478|
        +---+-------------------+-------+--------------------+

        We can use this transformation with windows.

        >>> feature_row_windows = Feature(
        ...    name="feature",
        ...    description="spark function transform with windows",
        ...    transformation=SparkFunctionTransform(
        ...       functions=[Function(functions.avg, DataType.DOUBLE)],)
        ...                    .with_window(partition_by="id",
        ...                                 mode="row_windows",
        ...                                 window_definition=["2 events"],
        ...   )
        ...)
        >>> feature_row_windows.transform(df).orderBy("timestamp").show()
        +--------+-----------------------+---------------------------------------+
        |feature | id|          timestamp| feature_avg_over_2_events_row_windows|
        +--------+---+-------------------+--------------------------------------+
        |     200|  1|2016-04-11 11:31:11|                                 200.0|
        |     300|  1|2016-04-11 11:44:12|                                 250.0|
        |     400|  1|2016-04-11 11:46:24|                                 350.0|
        |     500|  1|2016-04-11 12:03:21|                                 450.0|
        +--------+---+-------------------+--------------------------------------+

        It's important to notice that transformation doesn't affect the
        dataframe granularity.

    """

    def __init__(self, functions: List[Function]):
        super().__init__()
        self.functions = functions
        self._windows: List[Any] = []

    def with_window(
        self,
        partition_by: str,
        window_definition: List[str],
        order_by: str = None,
        mode: str = None,
    ) -> "SparkFunctionTransform":
        """Create a list with windows defined."""
        if mode is not None:
            self._windows = [
                Window(
                    partition_by=partition_by,
                    order_by=order_by,
                    mode=mode,
                    window_definition=definition,
                )
                for definition in window_definition
            ]
        return self

    def _get_output_name(self, function: object, window: Window = None) -> str:
        base_name = (
            "__".join([self._parent.name, function.__name__])
            if hasattr(function, "__name__")
            else self._parent.name
        )

        if self._windows and window is not None:
            return "_".join([base_name, window.get_name()])

        return base_name

    @property
    def output_columns(self) -> List[str]:
        """Columns generated by the transformation."""
        output_columns = []
        for function in self.functions:
            if self._windows:
                for window in self._windows:
                    output_columns.append(self._get_output_name(function.func, window))
            else:
                output_columns.append(self._get_output_name(function.func))

        return output_columns

    def transform(self, dataframe: DataFrame) -> DataFrame:
        """Performs a transformation to the feature pipeline.

        Args:
            dataframe: input dataframe.

        Returns:
            Transformed dataframe.
        """
        for function in self.functions:
            if self._windows:
                for window in self._windows:
                    dataframe = dataframe.withColumn(
                        self._get_output_name(function.func, window),
                        function.func(self._parent.from_column or self._parent.name)
                        .over(window.get())
                        .cast(function.data_type.spark),
                    )
            else:
                dataframe = dataframe.withColumn(
                    self._get_output_name(function.func),
                    function.func(self._parent.from_column or self._parent.name).cast(
                        function.data_type.spark
                    ),
                )

        return dataframe
