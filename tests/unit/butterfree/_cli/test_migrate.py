from butterfree._cli import migrate
from butterfree.pipelines import FeatureSetPipeline


def test_migrate_success():
    all_fs = migrate.migrate("tests/mocks/entities/")
    assert all(isinstance(fs, FeatureSetPipeline) for fs in all_fs)
    assert sorted([fs.feature_set.name for fs in all_fs]) == ["first", "second"]
