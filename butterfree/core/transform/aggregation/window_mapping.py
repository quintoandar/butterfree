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
        for name, type in WindowType.__members__.items():
            if str(window_type).upper() == name:
                return type.value * window_lenght
