from butterfree.core.reader import filter_dataframe, FileReader
from tests.unit.core.reader.conftest import feature_set_dataframe


def with_(df):
    # arrange
    file_reader = FileReader("test", "path/to/file", "format")

    transformation = {
                    "transformer": filter_dataframe,
                    "args": (feature_set_dataframe, "feature = 100"),
                    "kwargs": {},
                }
    # act
    file_reader.with_(
                transformation["transformer"],
                *transformation["args"],
                **transformation["kwargs"],
            )

    # assert
    print(file_reader.transformations)


with_(feature_set_dataframe)
