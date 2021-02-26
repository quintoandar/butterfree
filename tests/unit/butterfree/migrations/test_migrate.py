from butterfree.migrations import Migrate


class TestMigrate:
    def test_migrate(self, feature_set_pipeline, mocker):
        # given
        m = Migrate([feature_set_pipeline, feature_set_pipeline])
        m.migration = mocker.stub("migration")

        # when
        m.migration()

        # then
        m.migration.assert_called_once()

    def test_parse_feature_set_pipeline(self, feature_set_pipeline):

        # when
        m = Migrate._parse_feature_set_pipeline(feature_set_pipeline)

        # then
        assert m == [
            (feature_set_pipeline.sink.writers[0], feature_set_pipeline.feature_set,)
        ]
