from typing import Dict, List

from parameters_validation import non_blank
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from butterfree.core.transform.aggregation.window_mapping import WindowType
from butterfree.core.transform.feature_component import FeatureComponent
from butterfree.core.transform.aggregation.aggregation_mapping import get_agg


class Aggregation(FeatureComponent):
    def __init__(
        self,
        aggregations: non_blank(List[str]),
        windows: non_blank(Dict),
        partition: non_blank(str) = None,
        time_column: str = None,
    ):
        self._aggregations = (aggregations,)
        self._windows = (windows,)
        self._partition = partition
        self._parent = None
        self._time_column = time_column or "timestamp"

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, parent):
        self._parent = parent

    def transform(self, dataframe: DataFrame):
        for aggregation in self._aggregations[0]:
            for window_type, window_lenght in self._windows[0].items():
                feature_name = f"{self._parent.name[0]}__{aggregation}_over_{str(window_lenght)}_{window_type}"
                w = (
                    Window()
                    .partitionBy(F.col(f"{self._partition}"))
                    .orderBy(F.col(f"{self._time_column}").cast("long"))
                    .rangeBetween(
                        -(WindowType.convert_to_seconds(window_type, window_lenght)), 0
                    )
                )

                dataframe = dataframe.select(F.col("*")).withColumn(
                    feature_name, get_agg(aggregation, f"{self._parent.name[0]}").over(w)
                )

        return dataframe
