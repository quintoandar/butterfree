"""Explode json column for dataframes."""
from pyspark.sql.dataframe import DataFrame, StructType
from pyspark.sql.functions import from_json, get_json_object

JSON_TYPE_NAMES = ["array", "struct"]


def explode_json_column(
    df: DataFrame, column: str, json_schema: StructType
) -> DataFrame:
    """Create new columns extracting properties from a JSON column.

    Args:
        df: input dataframe with the target JSON column.
        column: column name that is going to be exploded.
        json_schema: expected schema from that JSON column.
            Not all "first layer" fields need to be mapped in the json_schema,
            just the desired columns. If there is any JSON field that is needed
            to be cast to a struct, the declared expected schema (a StructType)
            need to have the exact same schema as the presented record, if don't,
            the value in the resulting column will be null.

    Returns:
        dataframe with the new extracted columns from the JSON column.

    """
    for field in json_schema:
        if field.dataType.typeName() in JSON_TYPE_NAMES:
            df = df.withColumn(
                field.name,
                from_json(
                    get_json_object(df[column], "$.{}".format(field.name)),
                    schema=field.dataType,
                ),
            )
        else:  # non-collection data types
            df = df.withColumn(
                field.name,
                get_json_object(df[column], "$.{}".format(field.name)).cast(
                    field.dataType
                ),
            )
    return df
