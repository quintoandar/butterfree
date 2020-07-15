from butterfree.extract.pre_processing import forward_fill


class TestForwardFillTransform:
    def test_forward_fill_transform(self, input_df):
        # given
        result_df = forward_fill(
            dataframe=input_df,
            partition_by=["id", "pivot_column"],
            order_by="ts",
            fill_column="has_feature",
        )

        # assert
        assert all(
            [r.has_feature == 1 for r in result_df.filter("pivot_column = 3").collect()]
        )

    def test_forward_fill_transform_id_partition(self, input_df):
        # given
        result_df = forward_fill(
            dataframe=input_df,
            partition_by=["id"],
            order_by="ts",
            fill_column="has_feature",
        )

        # assert
        assert (
            result_df.filter("pivot_column = 3").orderBy("ts").collect()[-1].has_feature
            == 0
        )

    def test_forward_fill_transform_new_column(self, input_df):
        # given
        result_df = forward_fill(
            dataframe=input_df,
            partition_by=["id"],
            order_by="ts",
            fill_column="has_feature",
            filled_column="has_feature_filled",
        )

        # assert
        assert "has_feature_filled" in result_df.columns
        assert result_df.filter("has_feature_filled is null").count() == 0
