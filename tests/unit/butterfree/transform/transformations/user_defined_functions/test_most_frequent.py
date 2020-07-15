from pyspark.sql.types import ArrayType

from butterfree.testing.dataframe import assert_dataframe_equality
from butterfree.transform.transformations.user_defined_functions import (
    most_frequent_set,
)


def test_most_frequent_set_output(feature_set_dataframe, most_frequent_set_target_df):
    output_df = feature_set_dataframe.groupby("id").agg(most_frequent_set("feature1"))

    assert_dataframe_equality(output_df, most_frequent_set_target_df)


def test_most_frequent_set_str_input(
    feature_set_custom_dataframe, most_frequent_set_str_target_df
):
    output_df = feature_set_custom_dataframe.groupby("id").agg(
        most_frequent_set("feature1")
    )

    assert_dataframe_equality(output_df, most_frequent_set_str_target_df)


def test_most_frequent_set_output_type(
    feature_set_dataframe, most_frequent_set_target_df
):
    output_df = feature_set_dataframe.groupby("id").agg(most_frequent_set("feature1"))

    assert isinstance(
        output_df.schema["most_frequent_set(feature1)"].dataType, ArrayType
    )
