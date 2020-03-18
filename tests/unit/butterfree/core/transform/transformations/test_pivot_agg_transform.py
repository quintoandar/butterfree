import pytest
from tests.unit.butterfree.core.extract.pre_processing.conftest import (
    compare_dataframes,
)

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.transform.features import Feature
from butterfree.core.transform.transformations import PivotAggTransform


class TestAggregatedTransform:
    def test_pivot_transformation(
        self, input_df, pivot_df,
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=PivotAggTransform(
                group_by_columns=["id", TIMESTAMP_COLUMN],
                pivot_column="pivot_column",
                agg_column="id",
                aggregations=["count"],
            ),
        )

        result_df = test_feature.transform(input_df)

        # assert
        assert compare_dataframes(actual_df=result_df, expected_df=pivot_df,)

    def test_pivot_transform_output_columns(self, input_df):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=PivotAggTransform(
                group_by_columns=["id", TIMESTAMP_COLUMN],
                pivot_column="pivot_column",
                agg_column="id",
                aggregations=["count"],
            ),
        )

        df = test_feature.transform(input_df)

        assert all(
            [
                a == b
                for a, b in zip(
                    df.columns,
                    [
                        "id",
                        TIMESTAMP_COLUMN,
                        "1__count",
                        "2__count",
                        "3__count",
                        "4__count",
                    ],
                )
            ]
        )

    def test_pivot_transformation_with_forward_fill(
        self, input_df, pivot_ffill_df,
    ):
        test_feature = Feature(
            name="feature1",
            description="unit test",
            transformation=PivotAggTransform(
                group_by_columns=["id", TIMESTAMP_COLUMN],
                pivot_column="pivot_column",
                agg_column="id",
                aggregations=["count"],
                with_forward_fill=True,
            ),
        )

        result_df = test_feature.transform(input_df)

        # assert
        assert compare_dataframes(actual_df=result_df, expected_df=pivot_ffill_df,)

    def test_blank_aggregation(self, input_df):
        with pytest.raises(ValueError, match="Aggregations must not be empty."):
            Feature(
                name="feature1",
                description="unit test",
                transformation=PivotAggTransform(
                    group_by_columns=["id"],
                    pivot_column="pivot_column",
                    agg_column="id",
                    aggregations=[],
                    with_forward_fill=True,
                ),
            ).transform(input_df)
