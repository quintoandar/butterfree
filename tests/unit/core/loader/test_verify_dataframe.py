import pytest

from butterfree.core.loader.verify_dataframe import VerifyDataframe


class TestVerifyDataframe:
    def test_verify_without_column_ts(self, feature_set_without_ts):
        check = VerifyDataframe(feature_set_without_ts)

        with pytest.raises(ValueError):
            assert check.verify_column_ts()

    def test_verify_empty(self, feature_set_nullable):
        check = VerifyDataframe(feature_set_nullable)

        with pytest.raises(ValueError):
            assert check.verify_df_is_empty()
