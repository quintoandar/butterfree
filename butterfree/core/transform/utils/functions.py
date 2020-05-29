"""Utils for custom or spark function to generation namedtuple."""

from typing import Callable

from parameters_validation import non_blank

from butterfree.core.constants.data_type import DataType


class Functions:
    """Define a class Function.

    Like a namedtuple:
        Function = namedtuple("Function", ["function", "data_type"]).

    Attributes:
        function: custom or spark functions, such as avg, std, count.
            For more information check spark functions:
                'https://spark.apache.org/docs/2.3.1/api/python/_modules/pyspark/sql/functions.html'
            For custom functions, look the path:
                'butterfree/core/transform/transformations/user_defined_functions'.
        data_type: data type for the output columns.
    """

    def __init__(self, function: non_blank(callable), data_type: non_blank(DataType)):
        self.function = function
        self.data_type = data_type

    @property
    def function(self) -> Callable:
        """Function to be used in the transformation."""
        return self._function

    @function.setter
    def function(self, value: Callable):
        """Definitions to be used in the transformation."""
        if not value:
            raise ValueError("Function must not be empty.")
        if not isinstance(value, Callable):
            raise TypeError("Function must be callable.")

        self._function = value

    @property
    def data_type(self) -> DataType:
        """Function to be used in the transformation."""
        return self._data_type

    @data_type.setter
    def data_type(self, value: DataType):
        """Definitions to be used in the transformation."""
        if not value:
            raise ValueError("DataType must not be empty.")
        if not isinstance(value, DataType):
            raise TypeError("Data type must be DataType.")

        self._data_type = value
