import pytest

from butterfree.clients import SparkClient
from butterfree.hooks.schema_compatibility import SparkTableSchemaCompatibilityHook


class TestSparkTableSchemaCompatibilityHook:
    @pytest.mark.parametrize(
        "table, database, target_table_expression",
        [("table", "database", "`database`.`table`"), ("table", None, "`table`")],
    )
    def test_build_table_expression(self, table, database, target_table_expression):
        # arrange
        spark_client = SparkClient()

        # act
        result_table_expression = SparkTableSchemaCompatibilityHook(
            spark_client, table, database
        ).table_expression

        # assert
        assert target_table_expression == result_table_expression

    def test_run_compatible_schema(self, spark_session):
        # arrange
        spark_client = SparkClient()
        target_table = spark_session.sql(
            "select 1 as feature_a, 'abc' as feature_b, true as other_feature"
        )
        input_dataframe = spark_session.sql("select 1 as feature_a, 'abc' as feature_b")
        target_table.registerTempTable("test")

        hook = SparkTableSchemaCompatibilityHook(spark_client, "test")

        # act and assert
        assert hook.run(input_dataframe) == input_dataframe

    def test_run_incompatible_schema(self, spark_session):
        # arrange
        spark_client = SparkClient()
        target_table = spark_session.sql(
            "select 1 as feature_a, 'abc' as feature_b, true as other_feature"
        )
        input_dataframe = spark_session.sql(
            "select 1 as feature_a, 'abc' as feature_b, true as unregisted_column"
        )
        target_table.registerTempTable("test")

        hook = SparkTableSchemaCompatibilityHook(spark_client, "test")

        # act and assert
        with pytest.raises(ValueError, match="The dataframe has a schema incompatible"):
            hook.run(input_dataframe)
