from typing import Union, Tuple
from parameters_validation import non_blank
from pyspark.sql import DataFrame


class Feature:
    def __init__(
        self,
        *,
        name: non_blank(str),
        alias: str = None,
        origin: Union[str, Tuple[str]],
        data_type: str = None,
        description: non_blank(str),
    ):
        self._name = (name,)
        self._alias = (alias,)
        self._origin = (origin,)
        self._description = (description,)
        self._data_type = (data_type,)

    def transform(self, dataframe: DataFrame):
        if self._alias[0] is not None:
            return dataframe.withColumnRenamed(self._name[0], self._alias[0])
        return dataframe
