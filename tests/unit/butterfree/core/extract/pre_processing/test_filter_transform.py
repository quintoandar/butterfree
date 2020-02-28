import pytest
from testing import check_dataframe_equality

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.extract.pre_processing import filter
from butterfree.core.extract.readers import FileReader


class TestFilterDataFrame:
    def test_filter(self, filter_input_df, spark_context, spark_session):
        # given
        file_reader = FileReader("test", "path/to/file", "format")

        file_reader.with_(
            transformer=filter,
            condition="test not in ('fail') and feature in (110, 120)",
        )

        # when
        output_df = file_reader._apply_transformations(filter_input_df)

        target_data = [
            {"id": 1, TIMESTAMP_COLUMN: 1, "feature": 110, "test": "pass"},
            {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120, "test": "pass"},
        ]
        target_df = spark_session.read.json(spark_context.parallelize(target_data, 1))

        # then
        assert check_dataframe_equality(output_df, target_df)

    @pytest.mark.parametrize(
        "condition", [None, 100],
    )
    def test_filter_with_invalidations(
        self, target_df, condition, spark_context, spark_session
    ):
        # given
        file_reader = FileReader("test", "path/to/file", "format")

        file_reader.with_(transformer=filter, condition=condition)

        # then
        with pytest.raises(TypeError):
            file_reader._apply_transformations(target_df)
