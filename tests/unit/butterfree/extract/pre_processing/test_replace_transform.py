import pytest

from butterfree.extract.pre_processing import replace
from butterfree.testing.dataframe import (
    assert_dataframe_equality,
    create_df_from_collection,
)


class TestReplaceTransform:
    def test_replace(self, spark_context, spark_session):
        # arrange
        input_data = [
            {"id": 1, "type": "a"},
            {"id": 2, "type": "b"},
            {"id": 3, "type": "c"},
        ]
        target_data = [
            {"id": 1, "type": "type_a"},
            {"id": 2, "type": "type_b"},
            {"id": 3, "type": "c"},
        ]
        input_df = create_df_from_collection(input_data, spark_context, spark_session)
        target_df = create_df_from_collection(target_data, spark_context, spark_session)
        replace_dict = {"a": "type_a", "b": "type_b"}

        # act
        result_df = replace(input_df, "type", replace_dict)

        # assert
        assert_dataframe_equality(target_df, result_df)

    @pytest.mark.parametrize(
        "input_data, column, replace_dict",
        [
            ([{"column": "a"}], "not_column", {"a": "type_a"}),
            ([{"column": 123}], "column", {"a": "type_a"}),
            ([{"column": "a"}], "column", "not dict"),
            ([{"column": "a"}], "column", {"a": 1}),
        ],
    )
    def test_replace_with_invalid_args(
        self, input_data, column, replace_dict, spark_context, spark_session
    ):
        # arrange
        input_df = create_df_from_collection(input_data, spark_context, spark_session)

        # act and assert
        with pytest.raises(ValueError):
            replace(input_df, column, replace_dict)

    def test_replace_with_invalid_df(self):
        # act and assert
        with pytest.raises(
            ValueError, match="dataframe needs to be a Pyspark DataFrame type"
        ):
            replace(None, "col", {"a": "b"})
