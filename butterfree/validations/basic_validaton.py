"""Validation implementing basic checks over the dataframe."""

from pyspark.sql.dataframe import DataFrame

from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.validations.validation import Validation


class BasicValidation(Validation):
    """Basic validation suite for Feature Set's dataframe.

    Attributes:
        dataframe: object to be verified

    """

    def __init__(self, dataframe: DataFrame = None):
        super().__init__(dataframe)

    def check(self) -> None:
        """Check basic validation properties about the dataframe.

        Raises:
            ValueError: if any of the verifications fail

        """
        self.validate_df_is_spark_df()
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

    def validate_df_is_spark_df(self) -> None:
        """Check type of dataframe object.

        Raises:
            ValueError: if dataframe is not instance of pyspark.sql.DataFrame.

        """
        if not self.dataframe:
            raise ValueError("DataFrame can't be None.")
        if not isinstance(self.dataframe, DataFrame):
            raise ValueError(
                "dataframe needs to be a instance of pyspark.sql.DataFrame"
            )
