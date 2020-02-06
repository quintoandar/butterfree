from pyspark import SparkContext
from pyspark.sql import session

from butterfree.core.reader import filter_dataframe, FileReader
from tests.unit.core.reader.conftest import feature_set_dataframe


def with_(df):
    sc = SparkContext.getOrCreate()
    spark_client = session.SparkSession(sc)
    file_reader = FileReader("test", "path/to/file", "format")

    transformation = {
                    "transformer": filter_dataframe,
                    "args": (df, "feature = 100"),
                    "kwargs": {},
                }
    # act
    file_reader.with_(
                transformation["transformer"],
                *transformation["args"],
                **transformation["kwargs"],
            )

    result_df = file_reader._apply_transformations(df)

    # assert
    print(result_df.collect())


with_(feature_set_dataframe)
