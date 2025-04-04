from unittest.mock import MagicMock

import pytest
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.dataframe import DataFrame

from butterfree.validations import BasicValidation


def test_validate_without_column_ts(feature_set_without_ts):
    check = BasicValidation(feature_set_without_ts)

    with pytest.raises(ValueError):
        check.validate_column_ts()


def test_validate_df_is_empty_with_none_dataframe():
    validation = BasicValidation(None)

    with pytest.raises(ValueError, match="DataFrame can't be None."):
        validation.validate_df_is_empty()


def test_validate_df_is_empty_with_empty_dataframe(spark_session):
    df = spark_session.createDataFrame([], "id INT")
    validation = BasicValidation(df)

    with pytest.raises(ValueError, match="DataFrame can't be empty."):
        validation.validate_df_is_empty()


def test_validate_df_is_empty_with_non_empty_dataframe(spark_session):
    df = spark_session.createDataFrame([(1,)], "id INT")
    validation = BasicValidation(df)
    validation.validate_df_is_empty()


# If it's DBR < 13.3 (spark < 3.4.1) it will break. Every ConnectDataFrame has isEmpty
@pytest.mark.parametrize(
    "is_empty, has_is_empty, dataframe_type",
    [
        (True, True, DataFrame),
        (False, True, DataFrame),
        (True, False, DataFrame),
        (False, False, DataFrame),
        # This module `pyspark.sql.connect.dataframe.DataFrame` always has isEmpty
        # However, it does not have `rdd`
        (True, True, ConnectDataFrame),
        (False, True, ConnectDataFrame),
    ],
)
def test_is_empty_permutations(is_empty, has_is_empty, dataframe_type):
    df = MagicMock(spec=dataframe_type)

    if has_is_empty:
        df.isEmpty.return_value = is_empty
    else:
        delattr(df, "isEmpty")
        df.rdd.isEmpty.return_value = is_empty

    validation = BasicValidation(df)
    assert validation._is_empty() == is_empty
