import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame

from butterfree.constants.data_type import DataType

BUTTERFREE_DTYPES = {
    "string": DataType.STRING.spark_sql,
    "long": DataType.BIGINT.spark_sql,
    "double": DataType.DOUBLE.spark_sql,
    "boolean": DataType.BOOLEAN.spark_sql,
    "integer": DataType.INTEGER.spark_sql,
    "date": DataType.DATE.spark_sql,
    "timestamp": DataType.TIMESTAMP.spark_sql,
    "array": {
        "long": DataType.ARRAY_BIGINT.spark_sql,
        "float": DataType.ARRAY_FLOAT.spark_sql,
        "string": DataType.ARRAY_STRING.spark_sql,
    },
}


@dataclass(frozen=True)
class Table:  # noqa: D101
    id: str
    database: str
    name: str


class FeatureSetCreation:
    """Class to auto-generate readers and features."""

    def _get_features_with_regex(self, sql_query: str) -> List[str]:
        features = []
        sql_query = " ".join(sql_query.split())
        first_pattern = re.compile("[(]?([\w.*]+)[)]?,", re.IGNORECASE)
        second_pattern = re.compile("(\w+)\s(from)", re.IGNORECASE)

        for pattern in [first_pattern, second_pattern]:
            matches = pattern.finditer(sql_query)
            for match in matches:
                feature = match.group(1)

                if "." in feature:
                    feature = feature.split(".")[1]

                features.append(feature)

        return features

    def _get_data_type(self, field_name: str, df: DataFrame) -> str:
        for field in df.schema.jsonValue()["fields"]:
            if field["name"] == field_name:

                field_type = field["type"]

                if isinstance(field_type, dict):

                    field_type_keys = field_type.keys()

                    if "type" in field_type_keys and "elementType" in field_type_keys:
                        return (
                            "."
                            + BUTTERFREE_DTYPES[field_type["type"]][  # type: ignore
                                field_type["elementType"]
                            ]
                        )

                return "." + BUTTERFREE_DTYPES[field["type"]]

        return ""

    def _get_tables_with_regex(self, sql_query: str) -> Tuple[List[Table], str]:

        modified_sql_query = sql_query
        tables = []
        stop_words = [
            "left",
            "right",
            "full outer",
            "inner",
            "where",
            "join",
            "on",
            "as",
        ]
        keywords = ["from", "join"]

        for keyword in keywords:
            pattern = re.compile(
                rf"\b{keyword}\s+(\w+\.\w+|\w+)\s+(\w+)", re.IGNORECASE
            )
            matches = pattern.finditer(sql_query)

            for match in matches:
                full_table_name = match.group(1)
                id = match.group(2).strip()

                if id in stop_words:
                    id = full_table_name

                if "." in full_table_name:
                    database, table = full_table_name.split(".")

                    modified_sql_query = re.sub(
                        rf"\b{database}\.{table}\b", table, modified_sql_query
                    )

                    tables.append(Table(id=id, database=database, name=table))
                else:
                    modified_sql_query = re.sub(
                        rf"\b{full_table_name}\b", full_table_name, modified_sql_query
                    )
                    tables.append(Table(id=id, database="TBD", name=full_table_name))

        return tables, modified_sql_query

    def get_readers(self, sql_query: str) -> str:
        """
        Extracts table readers from a SQL query and formats them as a string.

        Args:
            sql_query (str): The SQL query from which to extract table readers.

        Returns:
            str: A formatted string containing the table readers.
        """
        tables, modified_sql_query = self._get_tables_with_regex(sql_query.lower())
        readers = []
        for table in tables:
            table_reader_string = f"""
            TableReader(
                id="{table.id}",
                database="{table.database}",
                table="{table.name}"
            ),
            """
            readers.append(table_reader_string)

        final_string = """
        source=Source(
            readers=[
            {}
            ],
            query=(
            \"\"\"
            {}
            \"\"\"
            ),
        ),
        """.format(
            "".join(readers), modified_sql_query.replace("\n", "\n\t\t")
        )

        return final_string

    def get_features(self, sql_query: str, df: Optional[DataFrame] = None) -> str:
        """
        Extract features from a SQL query and return them formatted as a string.

        Args:
            sql_query (str): The SQL query used to extract features.
            df (Optional[DataFrame], optional): Optional DataFrame used to infer data types. Defaults to None.

        Returns:
            str: A formatted string containing the extracted features.

        This sould be used on Databricks.

        Especially if you want automatic type inference without passing a reference dataframe.
        The utility will only work in an environment where a spark session is available in the environment
        """  # noqa: E501

        features = self._get_features_with_regex(sql_query)
        features_formatted = []
        for feature in features:
            description = feature.replace("__", " ").replace("_", " ").capitalize()

            data_type = "."

            if df is None:
                df = spark.sql(sql_query)  # type: ignore # noqa: F821

            data_type = self._get_data_type(feature, df)

            feature_string = f"""
            Feature(
            name="{feature}",
            description="{description}",
            dtype=DataType{data_type},
            ),
            """
            features_formatted.append(feature_string)

        final_string = ("features=[\t{}],\n),").format("".join(features_formatted))

        return final_string
