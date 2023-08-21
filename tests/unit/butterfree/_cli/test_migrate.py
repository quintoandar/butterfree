from unittest.mock import call

from typer.testing import CliRunner

from butterfree._cli import migrate
from butterfree._cli.main import app
from butterfree.migrations.database_migration import CassandraMigration
from butterfree.pipelines import FeatureSetPipeline

runner = CliRunner()


class TestMigrate:
    def test_migrate_success(self, mocker):
        all_fs = migrate.migrate("tests/mocks/entities/")
        assert all(isinstance(fs, FeatureSetPipeline) for fs in all_fs)
        assert sorted([fs.feature_set.name for fs in all_fs]) == ["first", "second"]

        mocker.patch.object(migrate.Migrate, "_send_logs_to_s3")

        all_fs = migrate.migrate("tests/mocks/entities/", False, False)

        assert CassandraMigration.apply_migration.call_count == 2

        cassandra_pairs = [
            call(pipe.feature_set, pipe.sink.writers[1], False) for pipe in all_fs
        ]
        CassandraMigration.apply_migration.assert_has_calls(
            cassandra_pairs, any_order=True
        )
        migrate.Migrate._send_logs_to_s3.assert_called_once()

    def test_app_cli(self):
        result = runner.invoke(app, "migrate")
        assert result.exit_code == 0

    def test_app_migrate(self, mocker):
        mocker.patch.object(migrate.Migrate, "run")
        result = runner.invoke(app, ["migrate", "apply", "tests/mocks/entities/"])
        assert result.exit_code == 0
