"""Validation implementing basic checks over the dataframe."""

from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

from pyspark.sql.dataframe import DataFrame

from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.validations.validation import Validation


class BasicValidation(Validation):
    """Basic validation suite for Feature Set's dataframe.

    Attributes:
        dataframe: object to be verified

    """

    def __init__(
        self, dataframe: Optional[Union["ConnectDataFrame", DataFrame]] = None
    ):
        super().__init__(dataframe)

    def check(self) -> None:
        """Check basic validation properties about the dataframe.

        Raises:
            ValueError: if any of the verifications fail

        """
        self.validate_column_ts()
        self.validate_df_is_empty()

    def validate_column_ts(self) -> None:
        """Check dataframe's ts column.

        Raises:
            ValueError: if dataframe don't have a column named ts.

        """
        if not self.dataframe:
            raise ValueError("DataFrame can't be None.")
        if TIMESTAMP_COLUMN not in self.dataframe.columns:
            raise ValueError(f"DataFrame must have a '{TIMESTAMP_COLUMN}' column.")

    def _is_empty(self) -> bool:
        if hasattr(self.dataframe, "isEmpty"):
            # pyspark >= 3.4
            return self.dataframe.isEmpty()
        # pyspark < 3.4
        return self.dataframe.rdd.isEmpty()

    def validate_df_is_empty(self) -> None:
        """Check dataframe emptiness.

        Raises:
            ValueError: if dataframe is empty and is not streaming.

        """

        if not self.dataframe:
            raise ValueError("DataFrame can't be None.")
        if (not self.dataframe.isStreaming) and self._is_empty():
            raise ValueError("DataFrame can't be empty.")
