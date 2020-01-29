from butterfree.core.configs import environment


class TestS3Config:
    def test_mode(self, s3_config):
        # expecting
        default = "overwrite"
        assert s3_config.mode == default

        # given
        s3_config.mode = None
        # then
        assert s3_config.mode == default

    def test_mode_custom(self, s3_config):
        # given
        mode = "append"
        s3_config.mode = mode

        # then
        assert s3_config.mode == mode

    def test_format(self, s3_config):
        # expecting
        default = "parquet"
        assert s3_config.format_ == default

        # given
        s3_config.format_ = None
        # then
        assert s3_config.format_ == default

    def test_format_custom(self, s3_config):
        # given
        format_ = "json"
        s3_config.format_ = format_

        # then
        assert s3_config.format_ == format_

    def test_path(self, s3_config):
        # expecting
        default = f"s3a://{environment.get_variable('FEATURE_STORE_S3_BUCKET')}"
        assert s3_config.path == default

    def test_path_custom(self, s3_config):
        # given
        path = "local/butterfree"
        s3_config.path = path

        # then
        assert s3_config.path == path

    def test_partition_by(self, s3_config):
        # expecting
        default = [
            "partition__year",
            "partition__month",
            "partition__day",
        ]
        assert s3_config.partition_by == default

        # given
        s3_config.partition_by = None
        # then
        assert s3_config.partition_by == default

    def test_partition_by_custom(self, s3_config):
        # given
        partition_by = "ts_column"
        s3_config.partition_by = partition_by

        # then
        assert s3_config.partition_by == partition_by
