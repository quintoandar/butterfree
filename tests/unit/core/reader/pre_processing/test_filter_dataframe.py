import pytest
from pyspark.sql.utils import AnalysisException

from butterfree.core.constant.columns import TIMESTAMP_COLUMN
from butterfree.core.reader import FileReader
from butterfree.core.reader.pre_processing import filter_dataframe


class TestFilterDataFrame:
    def test_filter(self, feature_set_dataframe, spark, sc):
        # given
        file_reader = FileReader("test", "path/to/file", "format")

        file_reader.with_(
            transformer=filter_dataframe,
            condition="test not in ('fail') and feature in (110, 120)",
        )

        # when
        result_df = file_reader._apply_transformations(feature_set_dataframe)

        transformation_df = [
            {"id": 1, TIMESTAMP_COLUMN: 1, "feature": 110, "test": "pass"},
            {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120, "test": "pass"},
        ]
        spark_df = spark.read.json(sc.parallelize(transformation_df, 1))

        # then
        assert result_df.collect() == spark_df.collect()

    def test_filter_with_column_not_exist(self, feature_set_dataframe, spark, sc):
        # given
        file_reader = FileReader("test", "path/to/file", "format")

        file_reader.with_(
            transformer=filter_dataframe, condition="column_not_exist = 100",
        )

        # then
        with pytest.raises(AnalysisException):
            file_reader._apply_transformations(feature_set_dataframe)

    @pytest.mark.parametrize(
        "condition", [None, 100],
    )
    def test_filter_with_invalidations(
        self, feature_set_dataframe, condition, spark, sc
    ):
        # given
        file_reader = FileReader("test", "path/to/file", "format")

        file_reader.with_(transformer=filter_dataframe, condition=condition)

        # then
        with pytest.raises(TypeError):
            file_reader._apply_transformations(feature_set_dataframe)
