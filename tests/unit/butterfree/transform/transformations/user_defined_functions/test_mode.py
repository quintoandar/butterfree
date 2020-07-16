from pyspark.sql.types import StringType

from butterfree.testing.dataframe import assert_dataframe_equality
from butterfree.transform.transformations.user_defined_functions import mode


def test_mode_output(feature_set_dataframe, mode_target_df):
    output_df = feature_set_dataframe.groupby("id").agg(mode("feature1"))

    assert_dataframe_equality(output_df, mode_target_df)


def test_mode_output_type(feature_set_dataframe, mode_target_df):
    output_df = feature_set_dataframe.groupby("id").agg(mode("feature1"))

    assert isinstance(output_df.schema["mode(feature1)"].dataType, StringType)
