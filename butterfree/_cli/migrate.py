import importlib
import inspect
import pkgutil
import sys
from typing import Set

import setuptools
import typer

from butterfree._cli import cli_logger
from butterfree.migrations.database_migration import ALLOWED_DATABASE
from butterfree.pipelines import FeatureSetPipeline

app = typer.Typer()


def __find_modules(path: str) -> Set[str]:
    modules = set()
    for pkg in setuptools.find_packages(path):
        modules.add(pkg)
        pkg_path = path + "/" + pkg.replace(".", "/")

        # different usage for older python3 versions
        if sys.version_info.minor < 6:
            for _, name, is_pkg in pkgutil.iter_modules([pkg_path]):
                if not is_pkg:
                    modules.add(pkg + "." + name)
        else:
            for info in pkgutil.iter_modules([pkg_path]):
                if not info.ispkg:
                    modules.add(pkg + "." + info.name)
    return modules


def __fs_objects(path: str) -> Set[FeatureSetPipeline]:
    cli_logger.info(f"Looking for python modules under {path}...")
    modules = __find_modules(path)
    if not modules:
        return set()

    cli_logger.info(f"Importing modules...")
    package = ".".join(path.strip("/").split("/"))
    imported = set(
        importlib.import_module(f".{name}", package=package) for name in modules
    )

    cli_logger.info(f"Scanning modules...")
    content = {
        module: set(
            filter(
                lambda x: not x.startswith("__"),  # filter "__any__" attributes
                set(item for item in dir(module)),
            )
        )
        for module in imported
    }

    instances = set()
    for module, items in content.items():
        for item in items:
            value = getattr(module, item)
            if not value:
                continue

            # filtering non-classes
            if not inspect.isclass(value):
                continue

            # filtering abstractions
            if inspect.isabstract(value):
                continue

            # filtering classes that doesn't inherit from FeatureSetPipeline
            if not issubclass(value, FeatureSetPipeline):
                continue

            # filtering FeatureSetPipeline itself
            if value == FeatureSetPipeline:
                continue

            instances.add(value)

    cli_logger.info("Creating instances...")
    return set(value() for value in instances)


PATH = typer.Argument(
    ..., help="Full or relative path to where feature set pipelines are being defined.",
)


class Migrate:
    """Execute migration operations in a Database based on pipeline Writer.

    Attributes:
        pipelines: list of Feature Set Pipelines to use to migration.
    """

    def __init__(self, pipelines: Set[FeatureSetPipeline]) -> None:
        self.pipelines = pipelines

    def _send_logs_to_s3(self) -> None:
        """Send all migration logs to S3."""
        pass

    def run(self) -> None:
        """Construct and apply the migrations."""
        for pipeline in self.pipelines:
            for writer in pipeline.sink.writers:
                migration = ALLOWED_DATABASE[writer.db_config.database]
                migration.apply_migration(pipeline.feature_set, writer)


@app.callback()
def migrate(path: str = PATH) -> Set[FeatureSetPipeline]:
    """Scan and run database migrations for feature set pipelines defined under PATH.

    Butterfree will scan a given path for classes that inherit from its
    FeatureSetPipeline and create dry instances of it to extract schema and writer
    information. By doing this, Butterfree can compare all defined feature set schemas
    to their current state on each sink being used.

    All pipelines must be under python modules inside path, so we can dynamically
    import and instantiate them.
    """
    pipe_set = __fs_objects(path)
    Migrate(pipe_set).run()
    return pipe_set
