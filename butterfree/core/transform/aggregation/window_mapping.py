"""Window mapping entity."""

from enum import Enum


class WindowType(Enum):
    """Enum Class defining the window sizes used in aggregations."""

    SECONDS = 1
    MINUTES = 60
    HOURS = 3600
    DAYS = 86400
    WEEKS = 604800
    MONTHS = 2419200
    YEARS = 29030400

    @staticmethod
    def convert_to_seconds(window_type, window_lenght):
        """Converts a specific time unit to seconds.

        Args:
            window_type: time unit.
            window_lenght: number of time units.

        Returns:
            time in seconds.
        """
        if window_type in ["seconds"]:
            return WindowType.SECONDS.value * window_lenght
        elif window_type in ["minutes"]:
            return WindowType.MINUTES.value * window_lenght
        elif window_type in ["hours"]:
            return WindowType.HOURS.value * window_lenght
        elif window_type in ["days"]:
            return WindowType.DAYS.value * window_lenght
        elif window_type in ["weeks"]:
            return WindowType.WEEKS.value * window_lenght
        elif window_type in ["months"]:
            return WindowType.MONTHS.value * window_lenght
        elif window_type in ["years"]:
            return WindowType.YEARS.value * window_lenght
        else:
            raise ValueError()
