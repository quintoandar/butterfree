import pytest

from butterfree.core.validations import BasicValidation


class TestBasicValidation:
    def test_validate_without_column_ts(self, feature_set_without_ts):
        check = BasicValidation(feature_set_without_ts)

        with pytest.raises(ValueError):
            check.validate_column_ts()

    def test_validate_empty(self, feature_set_empty):
        check = BasicValidation(feature_set_empty)

        with pytest.raises(ValueError):
            check.validate_df_is_empty()

    def test_validate_not_spark_df(self):
        df_writer = "not a spark df writer"
        check = BasicValidation(df_writer)

        with pytest.raises(ValueError):
            check.validate_df_is_spark_df()
