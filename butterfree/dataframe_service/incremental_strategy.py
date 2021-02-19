"""IncrementalStrategy entity."""

from __future__ import annotations

from pyspark.sql import DataFrame


class IncrementalStrategy:
    """Define an incremental strategy to be used on data sources.

    Entity responsible for defining a column expression that will be used to
    filter the original data source. The purpose is to get only the data related
    to a specific pipeline execution time interval.

    Attributes:
        column: column expression on which incremental filter will be applied.
            The expression need to result on a date or timestamp format, so the
            filter can properly work with the defined upper and lower bounds.
    """

    def __init__(self, column: str = None):
        self.column = column

    def from_milliseconds(self, column_name: str) -> IncrementalStrategy:
        """Create a column expression from ts column defined as milliseconds.

        Args:
            column_name: column name where the filter will be applied.

        Returns:
            `IncrementalStrategy` with the defined column expression.
        """
        return IncrementalStrategy(column=f"from_unixtime({column_name}/ 1000.0)")

    def from_string(self, column_name: str, mask: str = None) -> IncrementalStrategy:
        """Create a column expression from ts column defined as a simple string.

        Args:
            column_name: column name where the filter will be applied.
            mask: mask defining the date/timestamp format on the string.

        Returns:
            `IncrementalStrategy` with the defined column expression.
        """
        return IncrementalStrategy(column=f"to_date({column_name}, '{mask}')")

    def from_year_month_day_partitions(
        self,
        year_column: str = "year",
        month_column: str = "month",
        day_column: str = "day",
    ) -> IncrementalStrategy:
        """Create a column expression from year, month and day partitions.

        Args:
            year_column: column name from the year partition.
            month_column: column name from the month partition.
            day_column: column name from the day partition.

        Returns:
            `IncrementalStrategy` with the defined column expression.
        """
        return IncrementalStrategy(
            column=f"concat(string({year_column}), "
            f"'-', string({month_column}), "
            f"'-', string({day_column}))"
        )

    def get_expression(self, start_date: str = None, end_date: str = None) -> str:
        """Get the incremental filter expression using the defined dates.

        Both arguments can be set to defined a specific date interval, but  it's
        only necessary to set one of the arguments for this method to work.

        Args:
            start_date: date lower bound to use in the filter.
            end_date: date upper bound to use in the filter.

        Returns:
            Filter expression based on defined column and bounds.

        Raises:
            ValuerError: If both arguments, start_date and end_date, are None.
            ValueError: If the column expression was not defined.
        """
        if not self.column:
            raise ValueError("column parameter can't be None")
        if not (start_date or end_date):
            raise ValueError("Both arguments start_date and end_date can't be None.")
        if start_date:
            expression = f"date({self.column}) >= date('{start_date}')"
            if end_date:
                expression += f" and date({self.column}) <= date('{end_date}')"
            return expression
        return f"date({self.column}) <= date('{end_date}')"

    def filter_with_incremental_strategy(
        self, dataframe: DataFrame, start_date: str = None, end_date: str = None
    ) -> DataFrame:
        """Filters the dataframe according to the date boundaries.

        Args:
            dataframe: dataframe that will be filtered.
            start_date: date lower bound to use in the filter.
            end_date: date upper bound to use in the filter.

        Returns:
            Filtered dataframe based on defined time boundaries.
        """
        return (
            dataframe.where(
                self.get_expression(start_date=start_date, end_date=end_date)
            )
            if start_date or end_date
            else dataframe
        )
