from unittest.mock import call

from butterfree._cli import migrate
from butterfree.migrations.database_migration import (
    CassandraMigration,
    MetastoreMigration,
)
from butterfree.pipelines import FeatureSetPipeline

class TestMigrate:
    def test_migrate_success(self, mocker):
        mocker.patch.object(migrate.Migrate, "run")
        all_fs = migrate.migrate("tests/mocks/entities/")
        assert all(isinstance(fs, FeatureSetPipeline) for fs in all_fs)
        assert sorted([fs.feature_set.name for fs in all_fs]) == ["first", "second"]


    def test_migrate_all_pairs(self, mocker):
        mocker.patch.object(MetastoreMigration, "apply_migration")
        mocker.patch.object(CassandraMigration, "apply_migration")
        mocker.patch.object(migrate.Migrate, "_send_logs_to_s3")

        all_fs = migrate.migrate("tests/mocks/entities/")

        assert MetastoreMigration.apply_migration.call_count == 2
        assert CassandraMigration.apply_migration.call_count == 2

        metastore_pairs = [call(pipe.feature_set, pipe.sink.writers[0]) for pipe in all_fs]
        cassandra_pairs = [call(pipe.feature_set, pipe.sink.writers[1]) for pipe in all_fs]
        MetastoreMigration.apply_migration.assert_has_calls(metastore_pairs, any_order=True)
        CassandraMigration.apply_migration.assert_has_calls(cassandra_pairs, any_order=True)


    # def test_migrate_send_logs(self, mocker):
    #     mocker.patch.object(migrate.Migrate, "run")
    #     migrate.migrate("tests/mocks/entities/")
    #     migrate._send_logs_to_s3.assert_called_once()
