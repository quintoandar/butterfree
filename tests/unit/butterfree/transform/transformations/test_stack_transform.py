import pytest

from butterfree.constants import DataType
from butterfree.testing.dataframe import (
    assert_dataframe_equality,
    create_df_from_collection,
)
from butterfree.transform.features import Feature, KeyFeature
from butterfree.transform.transformations import StackTransform


class TestSQLExpressionTransform:

    input_data = [
        {"feature": 100, "id_a": 1, "id_b": 2},
        {"feature": 120, "id_a": 3, "id_b": 4},
    ]

    def test_feature_transform(self, spark_context, spark_session):
        # arrange
        target_data = [
            {"id": 1, "feature": 100, "id_a": 1, "id_b": 2},
            {"id": 2, "feature": 100, "id_a": 1, "id_b": 2},
            {"id": 3, "feature": 120, "id_a": 3, "id_b": 4},
            {"id": 4, "feature": 120, "id_a": 3, "id_b": 4},
        ]
        input_df = create_df_from_collection(
            self.input_data, spark_context, spark_session
        )
        target_df = create_df_from_collection(target_data, spark_context, spark_session)

        feature_using_names = KeyFeature(
            name="id",
            description="id_a and id_b stacked in a single column.",
            dtype=DataType.INTEGER,
            transformation=StackTransform("id_*"),
        )

        # act
        result_df_1 = feature_using_names.transform(input_df)

        # assert
        assert_dataframe_equality(target_df, result_df_1)

    def test_columns_not_in_dataframe(self, spark_context, spark_session):
        # arrange
        input_df = create_df_from_collection(
            self.input_data, spark_context, spark_session
        )

        feature = Feature(
            name="id",
            description="stack transformation",
            dtype=DataType.STRING,
            transformation=StackTransform("id_c", "id_d"),
        )

        # act and assert
        with pytest.raises(ValueError, match="Columns not found, columns in df: "):
            feature.transform(input_df)

    @pytest.mark.parametrize(
        "is_regex, pattern, column",
        [
            (False, "id_a", "id_a"),
            (False, "id_*", "id_a"),
            (False, "*_a", "id_a"),
            (False, "id*a", "id_a"),
            (False, "!id_b", "id_a"),
            (True, "id.*", "id_a"),
            (True, "id_[a-z]*", "id_column"),
        ],
    )
    def test__matches_pattern(self, is_regex, pattern, column):
        # arrange
        transform = StackTransform(is_regex=is_regex)

        # act
        result = transform._matches_pattern(pattern, column)

        # assert
        assert result
