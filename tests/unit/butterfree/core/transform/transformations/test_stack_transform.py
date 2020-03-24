import pytest

from butterfree.core.transform.features import Feature, KeyFeature
from butterfree.core.transform.transformations import StackTransform
from butterfree.testing.dataframe import (
    assert_dataframe_equality,
    create_df_from_collection,
)


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
            transformation=StackTransform(columns_names=["id_a", "id_b"]),
        )
        feature_using_prefix = KeyFeature(
            name="id",
            description="id_a and id_b stacked in a single column.",
            transformation=StackTransform(columns_prefix="id_"),
        )

        # act
        result_df_1 = feature_using_names.transform(input_df)
        result_df_2 = feature_using_prefix.transform(input_df)

        # assert
        assert_dataframe_equality(target_df, result_df_1)
        assert_dataframe_equality(target_df, result_df_2)

    def test_invalid_init_args(self, spark_context, spark_session):
        # arrange
        input_df = create_df_from_collection(
            self.input_data, spark_context, spark_session
        )

        # act and assert
        with pytest.raises(ValueError, match="both can't be None"):
            Feature(
                name="id",
                description="id_a and id_b stacked in a single column.",
                transformation=StackTransform(),
            )

        with pytest.raises(ValueError, match="not both"):
            Feature(
                name="id",
                description="id_a and id_b stacked in a single column.",
                transformation=StackTransform(
                    columns_names=["id_a", "id_b"], columns_prefix="id_"
                ),
            )

    def test_columns_not_in_dataframe(self, spark_context, spark_session):
        # arrange
        input_df = create_df_from_collection(
            self.input_data, spark_context, spark_session
        )

        feature_without_args = Feature(
            name="id",
            description="id_a and id_b stacked in a single column.",
            transformation=StackTransform(columns_names=["col_not_in_df"]),
        )
        feature_with_both_args = Feature(
            name="id",
            description="id_a and id_b stacked in a single column.",
            transformation=StackTransform(columns_prefix="wrong_prefix"),
        )

        # act and assert
        with pytest.raises(ValueError, match="target columns: "):
            feature_without_args.transform(input_df)

        with pytest.raises(ValueError, match="target columns prefix: "):
            feature_with_both_args.transform(input_df)
