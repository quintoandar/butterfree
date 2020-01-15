import pytest

from butterfree.core.source import FileSource


class TestFileSource:
    @pytest.mark.parametrize(
        "path, format, format_options",
        [
            ("path/to/file.parquet", "parquet", None),
            ("path/to/file.json", "json", None),
            (
                "path/to/file.json",
                "csv",
                {"sep": ",", "header": False, "inferSchema": True},
            ),
        ],
    )
    def test_get_data_from_file(
        self, path, format, format_options, spark_client, target_df
    ):
        # arrange
        spark_client.load.return_value = target_df
        file_source = FileSource(id="test", client=spark_client)

        # act
        output_df = file_source.get_data_from_file(path, format, format_options)

        # assert
        assert target_df == output_df

    @pytest.mark.parametrize(
        "path, format, format_options",
        [("path/to/file.parquet", None, None), (None, "json", None)],
    )
    def test_get_data_from_file_invalid_params(
        self, path, format, format_options, spark_client
    ):
        # arrange
        file_source = FileSource(id="test", client=spark_client)

        # act and assert
        with pytest.raises(ValueError):
            assert file_source.get_data_from_file(path, format, format_options)

    @pytest.mark.parametrize(
        "options",
        [
            ({"path": "path/to/file.parquet", "format": "parquet"}),
            ({"path": "path/to/file.json", "format": "json"}),
            (
                {
                    "path": "path/to/file.csv",
                    "format": "csv",
                    "format_options": {
                        "sep": ",",
                        "header": False,
                        "inferSchema": True,
                    },
                }
            ),
        ],
    )
    def test_consume(self, options, spark_client, target_df):
        # arrange
        spark_client.load.return_value = target_df
        file_source = FileSource(
            id="test", client=spark_client, consume_options=options
        )

        # act
        output_df = file_source.consume()

        # assert
        assert target_df == output_df

    @pytest.mark.parametrize(
        "options",
        [
            ({"wrong_param": "???", "format": "parquet"}),
            ({"path": "path/to/file.json", "wrong_param": "???"}),
            ("not a dict"),
        ],
    )
    def test_consume_invalid_params(self, options, spark_client):
        # arrange
        file_source = FileSource(
            id="test", client=spark_client, consume_options=options
        )

        # act and assert
        with pytest.raises(ValueError):
            assert file_source.consume()
