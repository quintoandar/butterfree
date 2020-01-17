import pytest

from butterfree.core.source import FileSource


class TestFileSource:
    @pytest.mark.parametrize(
        "path, format", [(None, "parquet"), ("path/to/file.json", 123), (123, None,)],
    )
    def test_init_invalid_params(self, path, format, spark_client):
        # act and assert
        with pytest.raises(ValueError):
            FileSource("id", spark_client, path, format)

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
    def test_consume(self, path, format, format_options, spark_client, target_df):
        # arrange
        spark_client.load.return_value = target_df
        file_source = FileSource("test", spark_client, path, format, format_options)

        # act
        output_df = file_source.consume()
        options = dict({"path": path}, **format_options if format_options else {})

        # assert
        spark_client.load.assert_called_once_with(format, options)
        assert target_df.collect() == output_df.collect()
