import pytest

from butterfree.core.dataframe import VerifyDataframe


class TestVerifyDataframe:
    def test_verify_without_column_ts(self, feature_set_without_ts):
        check = VerifyDataframe(feature_set_without_ts)

        with pytest.raises(ValueError):
            assert check.verify_column_ts()

    def test_verify_empty(self, feature_set_empty):
        check = VerifyDataframe(feature_set_empty)

        with pytest.raises(ValueError):
            assert check.verify_df_is_empty()

    def test_verify_not_spark_df(self):
        df_writer = "not a spark df writer"
        check = VerifyDataframe(df_writer)

        with pytest.raises(ValueError):
            assert check.verify_df_is_spark_df()
