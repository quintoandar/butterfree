import datetime
import importlib
import inspect
import json
import os
import pkgutil
import sys
from typing import Set

import boto3
import setuptools
import typer
from botocore.exceptions import ClientError

from butterfree.configs import environment
from butterfree.configs.logger import __logger
from butterfree.migrations.database_migration import ALLOWED_DATABASE
from butterfree.pipelines import FeatureSetPipeline

app = typer.Typer(help="Apply the automatic migrations in a database.")

logger = __logger("migrate", True)


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
        logger.error(f"Path: {path} not found!")
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

DEBUG_MODE = typer.Option(
    False,
    help="To view the queries resulting from the migration, DON'T apply the migration.",
)


class Migrate:
    """Execute migration operations in a Database based on pipeline Writer.

    Attributes:
        pipelines: list of Feature Set Pipelines to use to migration.
    """

    def __init__(self, pipelines: Set[FeatureSetPipeline],) -> None:
        self.pipelines = pipelines

    def _send_logs_to_s3(self, file_local: bool, debug_mode: bool) -> None:
        """Send all migration logs to S3."""
        file_name = "../logging.json"

        if not file_local and os.path.exists(file_name):
            s3_client = boto3.client("s3")

            timestamp = datetime.datetime.now()

            if debug_mode:
                object_name = (
                    f"logs/migrate-debug-mode/"
                    f"{timestamp.strftime('%Y-%m-%d')}"
                    f"/logging-{timestamp.strftime('%H:%M:%S')}.json"
                )
            else:
                object_name = (
                    f"logs/migrate/"
                    f"{timestamp.strftime('%Y-%m-%d')}"
                    f"/logging-{timestamp.strftime('%H:%M:%S')}.json"
                )
            bucket = environment.get_variable("FEATURE_STORE_S3_BUCKET")

            try:
                s3_client.upload_file(
                    file_name,
                    bucket,
                    object_name,
                    ExtraArgs={"ACL": "bucket-owner-full-control"},
                )
            except ClientError:
                raise

            os.remove(file_name)
        else:
            with open(file_name, "r") as json_f:
                json_data = json.load(json_f)
                print(json_data)

    def run(self, generate_logs: bool = False, debug_mode: bool = False) -> None:
        """Construct and apply the migrations."""
        for pipeline in self.pipelines:
            for writer in pipeline.sink.writers:
                db = writer.db_config.database
                if db == "cassandra":
                    migration = ALLOWED_DATABASE[db]
                    migration.apply_migration(pipeline.feature_set, writer, debug_mode)
                else:
                    logger.warning(f"Butterfree not supporting {db} Migrations yet.")

        self._send_logs_to_s3(generate_logs, debug_mode)


@app.command("apply")
def migrate(
    path: str = PATH, generate_logs: bool = GENERATE_LOGS, debug_mode: bool = DEBUG_MODE
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
    Migrate(pipe_set).run(generate_logs, debug_mode)
    return pipe_set
