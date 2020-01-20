import pytest

from butterfree.core.loader.verify_dataframe import verify_column_ts


class TestVerifyDataframe:

    def test_verify_without_column_ts(self, feature_set_without_ts):
        with pytest.raises(ValueError):
            assert verify_column_ts(feature_set_without_ts)

    def test_verify_column_ts(self, feature_set_dataframe):
        df = verify_column_ts(feature_set_dataframe)
        assert feature_set_dataframe == df
