"""Check dataframe's informations to write in Loader."""
from pyspark.sql.dataframe import DataFrame

from butterfree.core.constant.columns import TIMESTAMP_COLUMN


class VerifyDataframe:
    """Validate dataframe before to save.

    Attributes:
        dataframe: object to be verified

    """

    def __init__(self, dataframe: DataFrame):
        self.dataframe = dataframe

    def checks(self):
        """Call a set validate functions.

        Raises:
            ValueError: if any of the verifications fail

        """
        self.verify_df_is_spark_df()
        self.verify_column_ts()
        self.verify_df_is_empty()

    def verify_column_ts(self):
        """Check dataframe's ts column.

        Raises:
            ValueError: if dataframe don't have a column named ts

        """
        if TIMESTAMP_COLUMN not in self.dataframe.columns:
            raise ValueError(f"DataFrame must have a '{TIMESTAMP_COLUMN}' column.")

    def verify_df_is_empty(self):
        """Check dataframe emptiness.

        Raises:
            ValueError: if dataframe is empty

        """
        if self.dataframe.rdd.isEmpty():
            raise ValueError("DataFrame can't be empty.")

    def verify_df_is_spark_df(self):
        """Check type of dataframe object.

        Raises:
            ValueError: if dataframe is not instance of pyspark.sql.DataFrame.

        """
        if not isinstance(self.dataframe, DataFrame):
            raise ValueError(
                "dataframe needs to be a instance of pyspark.sql.DataFrame"
            )
