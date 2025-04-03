"""Validation implementing basic checks over the dataframe."""

from typing import Optional

from pyspark.sql.dataframe import DataFrame

from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.validations.validation import Validation


class BasicValidation(Validation):
    """Basic validation suite for Feature Set's dataframe.

    Attributes:
        dataframe: object to be verified

    """

    def __init__(self, dataframe: Optional[DataFrame] = None):
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

    def validate_df_is_empty(self) -> None:
        """Check dataframe emptiness.

        Raises:
            ValueError: if dataframe is empty and is not streaming.

        """
        if not self.dataframe:
            raise ValueError("DataFrame can't be None.")
        if (not self.dataframe.isStreaming) and self.dataframe.rdd.isEmpty():
            raise ValueError("DataFrame can't be empty.")
