"""Holds function for defining window in DataFrames."""
from typing import Any, List, Optional, Union

from pyspark import sql
from pyspark.sql import Column, WindowSpec, functions

from butterfree.constants.columns import TIMESTAMP_COLUMN
from butterfree.constants.window_definitions import ALLOWED_WINDOWS


class FrameBoundaries:
    """Utility functions for defining the frame boundaries.

    Args:
        mode: available modes to be used in time aggregations.
        window_definition: time ranges to be used in the windows,
        it can be second(s), minute(s), hour(s), day(s), week(s) and year(s),
    """

    def __init__(self, mode: Optional[str], window_definition: str):
        self.mode = mode
        self.window_definition = window_definition

    @property
    def window_size(self) -> int:
        """Returns window size."""
        if int(self.window_definition.split()[0]) <= 0:
            raise KeyError(f"{self.window_definition} have negative element.")
        return int(self.window_definition.split()[0])

    @property
    def window_unit(self) -> str:
        """Returns window unit."""
        unit = self.window_definition.split()[1]
        if unit not in ALLOWED_WINDOWS and self.mode != "row_windows":
            raise ValueError("Not allowed")

        return unit

    def get(self, window: WindowSpec) -> Any:
        """Returns window with or without the frame boundaries."""
        if self.mode is None:
            return window
        if self.mode == "row_windows":
            span = self.window_size - 1
            return window.rowsBetween(-span, 0)
        if self.mode == "fixed_windows":
            span = ALLOWED_WINDOWS[self.window_unit] * self.window_size
            return window.rangeBetween(-span, 0)


class Window:
    """Utility functions for defining a window specification.

    Args:
        partition_by: he partitioning defined.
        order_by: the ordering defined.
        mode: available modes to be used in time aggregations.
        window_definition: time ranges to be used in the windows, it can be second(s),
            minute(s), hour(s), day(s), week(s) and year(s),

    Use the static methods in :class:`Window` to create a :class:`WindowSpec`.
    """

    SLIDE_DURATION: str = "1 day"

    def __init__(
        self,
        window_definition: str,
        partition_by: Optional[Union[Column, str, List[str]]] = None,
        order_by: Optional[Union[Column, str]] = None,
        mode: str = None,
    ):
        self.partition_by = partition_by
        self.order_by = order_by or TIMESTAMP_COLUMN
        self.frame_boundaries = FrameBoundaries(mode, window_definition)

    def get_name(self) -> str:
        """Return window suffix name based on passed criteria."""
        return "_".join(
            [
                "over",
                f"{self.frame_boundaries.window_size}",
                f"{self.frame_boundaries.window_unit}",
                self.frame_boundaries.mode,
            ]
        )

    def get(self) -> Any:
        """Defines a common window to be used both in time and rows windows."""
        if self.frame_boundaries.mode == "rolling_windows":
            if int(self.frame_boundaries.window_definition.split()[0]) <= 0:
                raise KeyError(
                    f"{self.frame_boundaries.window_definition} "
                    f"have negative element."
                )
            return functions.window(
                TIMESTAMP_COLUMN,
                self.frame_boundaries.window_definition,
                slideDuration=self.SLIDE_DURATION,
            )
        elif self.order_by == TIMESTAMP_COLUMN:
            w = sql.Window.partitionBy(self.partition_by).orderBy(  # type: ignore
                functions.col(TIMESTAMP_COLUMN).cast("long")
            )
        else:
            w = sql.Window.partitionBy(self.partition_by).orderBy(  # type: ignore
                self.order_by
            )
        return self.frame_boundaries.get(w)
