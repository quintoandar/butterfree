import importlib
import inspect
import logging
import os
import pkgutil
import sys
from typing import Set

import setuptools
import typer

from butterfree.clients import SparkClient
from butterfree.configs import environment
from butterfree.extract.readers import FileReader
from butterfree.migrations.database_migration import ALLOWED_DATABASE
from butterfree.pipelines import FeatureSetPipeline

app = typer.Typer(help="Apply the automatic migrations in a database.")

logger = logging.getLogger("migrate")


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
    logger.info(f"Looking for python modules under {path}...")
    modules = __find_modules(path)
    if not modules:
        return set()

    logger.info(f"Importing modules...")
    package = ".".join(path.strip("/").split("/"))
    imported = set(
        importlib.import_module(f".{name}", package=package) for name in modules
    )

    logger.info(f"Scanning modules...")
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

    logger.info("Creating instances...")
    return set(value() for value in instances)


PATH = typer.Argument(
    ..., help="Full or relative path to where feature set pipelines are being defined.",
)

GENERATE_LOGS = typer.Option(
    False, help="To generate the logs in local file 'logging.json'."
)


class Migrate:
    """Execute migration operations in a Database based on pipeline Writer.

    Attributes:
        pipelines: list of Feature Set Pipelines to use to migration.
    """

    def __init__(
        self, pipelines: Set[FeatureSetPipeline], spark_client: SparkClient = None
    ) -> None:
        self.pipelines = pipelines
        self.spark_client = spark_client or SparkClient()

    def _send_logs_to_s3(self, file_local: bool) -> None:
        """Send all migration logs to S3."""
        file_reader = FileReader(id="name", path="logs/logging.json", format="json")
        df = file_reader.consume(self.spark_client)

        path = environment.get_variable("FEATURE_STORE_S3_BUCKET")

        self.spark_client.write_dataframe(
            dataframe=df,
            format_="json",
            mode="append",
            **{"path": f"s3a://{path}/logging"},
        )

        if not file_local:
            os.rmdir("logs/logging.json")

    def run(self, generate_logs: bool) -> None:
        """Construct and apply the migrations."""
        for pipeline in self.pipelines:
            for writer in pipeline.sink.writers:
                migration = ALLOWED_DATABASE[writer.db_config.database]
                migration.apply_migration(pipeline.feature_set, writer)

        self._send_logs_to_s3(generate_logs)


@app.command("apply")
def migrate(
    path: str = PATH, generate_logs: bool = GENERATE_LOGS,
) -> Set[FeatureSetPipeline]:
    """Scan and run database migrations for feature set pipelines defined under PATH.

    Butterfree will scan a given path for classes that inherit from its
    FeatureSetPipeline and create dry instances of it to extract schema and writer
    information. By doing this, Butterfree can compare all defined feature set schemas
    to their current state on each sink being used.

    All pipelines must be under python modules inside path, so we can dynamically
    import and instantiate them.
    """
    pipe_set = __fs_objects(path)
    Migrate(pipe_set).run(generate_logs)
    return pipe_set
