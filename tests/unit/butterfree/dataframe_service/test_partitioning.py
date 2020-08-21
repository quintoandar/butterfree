from butterfree.dataframe_service import extract_partition_values


class TestPartitioning:
    def test_extract_partition_values(self, test_partitioning_input_df):
        # arrange
        target_values = [
            {"year": 2009, "month": 8, "day": 20},
            {"year": 2020, "month": 8, "day": 20},
            {"year": 2020, "month": 9, "day": 20},
            {"year": 2020, "month": 8, "day": 21},
        ]

        # act
        result_values = extract_partition_values(
            test_partitioning_input_df, patition_columns=["year", "month", "day"]
        )

        # assert
        assert result_values == target_values
