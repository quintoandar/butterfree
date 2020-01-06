from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from quintoandar_butterfree.core.input import Input


class Source:
    def __init__(
        self, *, views: Dict[str, Input], query: str, spark: SparkSession = None
    ):
        self._query = query
        self._views = views
        self._spark = spark or SparkSession.builder.getOrCreate()

    def fetch(self) -> DataFrame:
        self._create_views()
        return self._run_query()

    def _create_views(self):
        for view_name, source_input in self._views.items():
            source_input.create_view(view_name)

    def _run_query(self) -> DataFrame:
        return self._spark.sql(self._query)
