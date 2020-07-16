from butterfree.configs import environment


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

    def test_bucket(self, s3_config):
        # expecting
        default = environment.get_variable("FEATURE_STORE_S3_BUCKET")
        assert s3_config.bucket == default

    def test_bucket_custom(self, s3_config):
        # given
        bucket = "test"
        s3_config.bucket = bucket

        # then
        assert s3_config.bucket == bucket
