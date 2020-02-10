import pytest

from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.reader import FileReader
from butterfree.core.reader.pre_processing import filter


class TestFilterDataFrame:
    def test_filter(self, feature_set_dataframe, spark, sc):
        # given
        file_reader = FileReader("test", "path/to/file", "format")

        file_reader.with_(
            transformer=filter,
            condition="test not in ('fail') and feature in (110, 120)",
        )

        # when
        result_df = file_reader._apply_transformations(feature_set_dataframe)

        target_data = [
            {"id": 1, TIMESTAMP_COLUMN: 1, "feature": 110, "test": "pass"},
            {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120, "test": "pass"},
        ]
        target_df = spark.read.json(sc.parallelize(target_data, 1))

        # then
        assert result_df.collect() == target_df.collect()

    @pytest.mark.parametrize(
        "condition", [None, 100],
    )
    def test_filter_with_invalidations(
        self, feature_set_dataframe, condition, spark, sc
    ):
        # given
        file_reader = FileReader("test", "path/to/file", "format")

        file_reader.with_(transformer=filter, condition=condition)

        # then
        with pytest.raises(TypeError):
            file_reader._apply_transformations(feature_set_dataframe)
