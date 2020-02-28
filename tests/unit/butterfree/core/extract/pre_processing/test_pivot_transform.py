import pytest
from pyspark.sql.functions import first
from testing import check_dataframe_equality

from butterfree.core.extract.pre_processing import pivot
from butterfree.core.extract.readers import FileReader


class TestPivotTransform:
    def test_pivot_transformation(
        self, input_df, pivot_df,
    ):
        output_df = pivot(
            dataframe=input_df,
            group_by_columns=["id", "ts"],
            pivot_column="pivot_column",
            agg_column="has_feature",
            aggregation=first,
        )

        # assert
        assert check_dataframe_equality(output_df, pivot_df)

    def test_pivot_transformation_with_forward_fill(
        self, input_df, pivot_ffill_df,
    ):
        output_df = pivot(
            dataframe=input_df,
            group_by_columns=["id", "ts"],
            pivot_column="pivot_column",
            agg_column="has_feature",
            aggregation=first,
            with_forward_fill=True,
        )

        # assert
        assert check_dataframe_equality(output_df, pivot_ffill_df)

    def test_pivot_transformation_with_forward_fill_and_mock(
        self, input_df, pivot_ffill_mock_df,
    ):
        output_df = pivot(
            dataframe=input_df,
            group_by_columns=["id", "ts"],
            pivot_column="pivot_column",
            agg_column="has_feature",
            aggregation=first,
            mock_value=-1,
            mock_type="int",
            with_forward_fill=True,
        )

        # assert
        assert check_dataframe_equality(output_df, pivot_ffill_mock_df)

    def test_pivot_transformation_mock_without_type(
        self, input_df, pivot_ffill_mock_df,
    ):
        with pytest.raises(AttributeError):
            _ = pivot(
                dataframe=input_df,
                group_by_columns=["id", "ts"],
                pivot_column="pivot_column",
                agg_column="has_feature",
                aggregation=first,
                mock_value=-1,
                with_forward_fill=True,
            )

    def test_apply_pivot_transformation(self, input_df, pivot_df):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")
        file_reader.with_(
            transformer=pivot,
            group_by_columns=["id", "ts"],
            pivot_column="pivot_column",
            agg_column="has_feature",
            aggregation=first,
        )

        # act
        output_df = file_reader._apply_transformations(input_df)

        # assert
        assert check_dataframe_equality(output_df, pivot_df)
