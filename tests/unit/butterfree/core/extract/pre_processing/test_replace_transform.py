import pytest

from butterfree.core.constants.columns import TIMESTAMP_COLUMN
from butterfree.core.extract.pre_processing import replace
from butterfree.core.extract.readers import FileReader


class TestReplaceTransform:
    def test_replace(self, feature_set_dataframe, spark_context, spark_session):
        # arrange
        file_reader = FileReader("test", "path/to/file", "format")

        file_reader.with_(
            transformer=filter,
            condition="test not in ('fail') and feature in (110, 120)",
        )

        # act
        result_df = file_reader._apply_transformations(feature_set_dataframe)

        target_data = [
            {"id": 1, TIMESTAMP_COLUMN: 1, "feature": 110, "test": "pass"},
            {"id": 1, TIMESTAMP_COLUMN: 2, "feature": 120, "test": "pass"},
        ]
        target_df = spark_session.read.json(spark_context.parallelize(target_data, 1))

        # assert
        assert result_df.collect() == target_df.collect()

    @pytest.mark.parametrize(
        "condition", [None, 100],
    )
    def test_replace_with_invalidations(
        self, feature_set_dataframe, condition, spark_context, spark_session
    ):
        # given
        file_reader = FileReader("test", "path/to/file", "format")

        file_reader.with_(transformer=filter, condition=condition)

        # then
        with pytest.raises(TypeError):
            file_reader._apply_transformations(feature_set_dataframe)
