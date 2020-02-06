from pyspark.sql.functions import first

from butterfree.core.reader import FileReader
from butterfree.core.reader.pre_processing.pivot_transform import pivot_table


class TestPivotTransform:
    def test_pivot_transformation(
        self, pivot_df, target_pivot_df,
    ):
        df = (
            pivot_table(
                dataframe=pivot_df,
                group_by_columns=["id", "ts"],
                pivot_column="pivot_column",
                aggregation_expression=first("has_feature"),
            )
            .orderBy("ts")
            .collect()
        )

        target_df = target_pivot_df.collect()

        # assert
        for line in range(0, 4):
            assert df[line]["id"] == target_df[line]["id"]
            assert df[line]["ts"] == target_df[line]["ts"]
            assert df[line]["1"] == target_df[line]["1"]
            assert df[line]["2"] == target_df[line]["2"]
            assert df[line]["3"] == target_df[line]["3"]
            assert df[line]["4"] == target_df[line]["4"]

    def test_apply_pivot_transformation(self, pivot_df, target_pivot_df):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")
        file_reader.with_(
            transformer=pivot_table,
            group_by_columns=["id", "ts"],
            pivot_column="pivot_column",
            aggregation_expression=first("has_feature"),
        )

        # act
        result_df = sorted(file_reader._apply_transformations(pivot_df).collect())
        target_df = target_pivot_df.collect()

        # assert
        for line in range(0, 4):
            assert result_df[line]["id"] == target_df[line]["id"]
            assert result_df[line]["ts"] == target_df[line]["ts"]
            assert result_df[line]["1"] == target_df[line]["1"]
            assert result_df[line]["2"] == target_df[line]["2"]
            assert result_df[line]["3"] == target_df[line]["3"]
            assert result_df[line]["4"] == target_df[line]["4"]
