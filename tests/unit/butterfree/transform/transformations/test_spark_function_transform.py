import pytest
from pyspark.sql import functions

from butterfree.constants import DataType
from butterfree.testing.dataframe import assert_dataframe_equality
from butterfree.transform.features import Feature
from butterfree.transform.transformations import SparkFunctionTransform
from butterfree.transform.utils import Function


class TestSparkFunctionTransform:
    def test_feature_transform(self, feature_set_dataframe, target_df_spark):
        test_feature = Feature(
            name="feature",
            description="unit test",
            transformation=SparkFunctionTransform(
                functions=[Function(functions.cos, DataType.DOUBLE)],
            ),
            from_column="feature1",
        )

        output_df = test_feature.transform(feature_set_dataframe)

        assert_dataframe_equality(output_df, target_df_spark)

    def test_feature_transform_with_window(
        self, feature_set_dataframe, target_df_rows_agg
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=SparkFunctionTransform(
                functions=[Function(functions.avg, DataType.DOUBLE)],
            ).with_window(
                partition_by="id",
                mode="row_windows",
                window_definition=["2 events", "3 events"],
            ),
        )

        output_df = test_feature.transform(feature_set_dataframe)

        assert_dataframe_equality(output_df, target_df_rows_agg)

    def test_output_columns(self):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=SparkFunctionTransform(
                functions=[Function(functions.avg, DataType.DOUBLE)],
            ).with_window(
                partition_by="id",
                mode="fixed_windows",
                window_definition=["7 days", "2 weeks"],
            ),
        )

        df_columns = test_feature.get_output_columns()

        assert all(
            [
                a == b
                for a, b in zip(
                    df_columns,
                    [
                        "feature1__avg_over_7_days_fixed_windows",
                        "feature1__avg_over_2_weeks_fixed_windows",
                    ],
                )
            ]
        )

    def test_unsupported_windows(self, feature_set_dataframe):
        with pytest.raises(ValueError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=SparkFunctionTransform(
                    functions=[Function(functions.avg, DataType.DOUBLE)],
                ).with_window(
                    partition_by="id",
                    mode="fixed_windows",
                    window_definition=["7 daily"],
                ),
            ).transform(feature_set_dataframe)

    def test_negative_windows(self, feature_set_dataframe):
        with pytest.raises(KeyError):
            Feature(
                name="feature1",
                description="unit test",
                transformation=SparkFunctionTransform(
                    functions=[Function(functions.avg, DataType.DOUBLE)],
                ).with_window(
                    partition_by="id",
                    mode="fixed_windows",
                    window_definition=["-2 weeks"],
                ),
            ).transform(feature_set_dataframe)

    def test_feature_transform_output_fixed_windows(
        self, feature_set_dataframe, target_df_fixed_agg
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=SparkFunctionTransform(
                functions=[Function(functions.avg, DataType.DOUBLE)],
            ).with_window(
                partition_by="id",
                mode="fixed_windows",
                window_definition=["2 minutes", "15 minutes"],
            ),
        )

        output_df = test_feature.transform(feature_set_dataframe)

        assert_dataframe_equality(output_df, target_df_fixed_agg)

    def test_feature_transform_output_row_windows(
        self, feature_set_dataframe, target_df_rows_agg_2
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=SparkFunctionTransform(
                functions=[Function(functions.avg, DataType.DOUBLE)],
            ).with_window(
                partition_by="id", mode="row_windows", window_definition=["2 events"],
            ),
        )

        output_df = test_feature.transform(feature_set_dataframe)

        assert_dataframe_equality(output_df, target_df_rows_agg_2)
