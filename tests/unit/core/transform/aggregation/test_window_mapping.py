from butterfree.core.transform.aggregation.window_mapping import WindowType


class TestWindowMapping:
    def test_outputs(self):
        seconds = WindowType.convert_to_seconds("seconds", 1)
        minutes = WindowType.convert_to_seconds("minutes", 1)
        hours = WindowType.convert_to_seconds("hours", 1)
        days = WindowType.convert_to_seconds("days", 1)
        weeks = WindowType.convert_to_seconds("weeks", 1)
        months = WindowType.convert_to_seconds("months", 1)
        years = WindowType.convert_to_seconds("years", 1)

        assert seconds == WindowType.SECONDS.value
        assert minutes == WindowType.MINUTES.value
        assert hours == WindowType.HOURS.value
        assert days == WindowType.DAYS.value
        assert weeks == WindowType.WEEKS.value
        assert months == WindowType.MONTHS.value
        assert years == WindowType.YEARS.value
