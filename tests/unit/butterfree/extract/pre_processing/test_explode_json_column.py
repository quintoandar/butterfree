from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from butterfree.extract.pre_processing import explode_json_column
from butterfree.testing.dataframe import (
    assert_dataframe_equality,
    create_df_from_collection,
)


def test_explode_json_column(spark_context, spark_session):
    # arrange
    input_data = [{"json_column": '{"a": 123, "b": "abc", "c": "123", "d": [1, 2, 3]}'}]
    target_data = [
        {
            "json_column": '{"a": 123, "b": "abc", "c": "123", "d": [1, 2, 3]}',
            "a": 123,
            "b": "abc",
            "c": 123,
            "d": [1, 2, 3],
        }
    ]

    input_df = create_df_from_collection(input_data, spark_context, spark_session)
    target_df = create_df_from_collection(target_data, spark_context, spark_session)

    json_column_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", IntegerType()),
            StructField("d", ArrayType(IntegerType())),
        ]
    )

    # act
    output_df = explode_json_column(input_df, "json_column", json_column_schema)

    # arrange
    assert_dataframe_equality(target_df, output_df)
