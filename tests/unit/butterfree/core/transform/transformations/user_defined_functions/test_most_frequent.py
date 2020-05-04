from pyspark.sql.types import ArrayType

from butterfree.core.transform.transformations.user_defined_functions import (
    most_frequent_elements_list,
)
from butterfree.testing.dataframe import assert_dataframe_equality


def test_most_frequent_output(feature_set_dataframe, most_frequent_target_df):
    output_df = feature_set_dataframe.groupby("id").agg(
        most_frequent_elements_list("feature1")
    )

    assert_dataframe_equality(output_df, most_frequent_target_df)


def test_most_frequent_output_type(feature_set_dataframe, most_frequent_target_df):
    output_df = feature_set_dataframe.groupby("id").agg(
        most_frequent_elements_list("feature1")
    )

    assert isinstance(
        output_df.schema["most_frequent_elements_list(feature1)"].dataType, ArrayType
    )
