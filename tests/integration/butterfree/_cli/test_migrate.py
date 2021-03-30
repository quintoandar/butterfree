from unittest.mock import call

from butterfree._cli import migrate
from butterfree.migrations.database_migration import (
    CassandraMigration,
    MetastoreMigration,
)
from butterfree.pipelines import FeatureSetPipeline


def test_migrate_success():
    migrate.migrate("tests/mocks/entities/", generate_logs=True)
