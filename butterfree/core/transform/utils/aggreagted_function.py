"""Utils for aggregated function to generation namedtuple."""

from parameters_validation import non_blank

from butterfree.core.constants.data_type import DataType


class Function:
    """Define a class Function.

    Attributes:
        function: aggregation functions, such as avg, std, count.
        data_type: data type for the output columns.

    Like a namedtuple:
        Function = namedtuple("Function", ["function", "data_type"]).
    """

    def __init__(self, function: non_blank(str), data_type: non_blank(DataType)):
        self.function = function
        self.data_type = data_type
