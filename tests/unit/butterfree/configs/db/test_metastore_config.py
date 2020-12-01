from butterfree.configs import environment


class TestMetastoreConfig:
    def test_mode(self, metastore_config):
        # expecting
        default = "overwrite"
        assert metastore_config.mode == default

        # given
        metastore_config.mode = None
        # then
        assert metastore_config.mode == default

    def test_mode_custom(self, metastore_config):
        # given
        mode = "append"
        metastore_config.mode = mode

        # then
        assert metastore_config.mode == mode

    def test_format(self, metastore_config):
        # expecting
        default = "parquet"
        assert metastore_config.format_ == default

        # given
        metastore_config.format_ = None
        # then
        assert metastore_config.format_ == default

    def test_format_custom(self, metastore_config):
        # given
        format_ = "json"
        metastore_config.format_ = format_

        # then
        assert metastore_config.format_ == format_

    def test_path(self, metastore_config):
        # expecting
        default = environment.get_variable("FEATURE_STORE_S3_BUCKET")
        assert metastore_config.path == default

    def test_path_custom(self, metastore_config):
        # given
        bucket = "test"
        metastore_config.path = bucket

        # then
        assert metastore_config.path == bucket

    def test_file_system(self, metastore_config):
        # expecting
        default = "s3a"
        assert metastore_config.file_system == default

    def test_file_system_custom(self, metastore_config):
        # given
        file_system = "dbfs"
        metastore_config.file_system = file_system

        # then
        assert metastore_config.file_system == file_system
