import logging

from delta.tables import DeltaTable
from pyspark.sql.dataframe import DataFrame

from butterfree.clients import SparkClient

logger = logging.getLogger(__name__)


class DeltaWriter:
    """Control operations on Delta Tables.

    Resposible for merging and optimizing.
    """

    @staticmethod
    def _get_full_table_name(table, database):
        if database:
            return "{}.{}".format(database, table)
        else:
            return table

    @staticmethod
    def _convert_to_delta(client: SparkClient, table: str):
        logger.info(f"Converting {table} to Delta...")
        client.conn.sql(f"CONVERT TO DELTA {table}")
        logger.info("Conversion done.")

    @staticmethod
    def merge(
        client: SparkClient,
        database: str,
        table: str,
        merge_on: list,
        source_df: DataFrame,
        when_not_matched_insert_condition: str = None,
        when_matched_update_condition: str = None,
        when_matched_delete_condition: str = None,
    ):
        """
        Merge a source dataframe to a Delta table.

        By default, it will update when matched, and insert when
        not matched (simple upsert).

        You can change this behavior by setting:
        - when_not_matched_insert_condition: it will only insert
            when this specified condition is true
        - when_matched_update_condition: it will only update when this
            specified condition is true. You can refer to the columns
        in the source dataframe as source.<column_name>, and the columns
            in the target table as target.<column_name>.
        - when_matched_delete_condition: it will add an operation to delete,
            but only if this condition is true. Again, source and
            target dataframe columns can be referred to respectively as
            source.<column_name> and target.<column_name>
        """
        try:
            full_table_name = DeltaWriter._get_full_table_name(table, database)

            table_exists = client.conn.catalog.tableExists(full_table_name)

            if table_exists:
                pd_df = client.conn.sql(
                    f"DESCRIBE TABLE EXTENDED {full_table_name}"
                ).toPandas()
                provider = (
                    pd_df.reset_index()
                    .groupby(["col_name"])["data_type"]
                    .aggregate("first")
                    .Provider
                )
                table_is_delta = provider.lower() == "delta"

                if not table_is_delta:
                    DeltaWriter()._convert_to_delta(client, full_table_name)

            # For schema evolution
            client.conn.conf.set(
                "spark.databricks.delta.schema.autoMerge.enabled", "true"
            )

            target_table = DeltaTable.forName(client.conn, full_table_name)
            join_condition = " AND ".join(
                [f"source.{col} = target.{col}" for col in merge_on]
            )
            merge_builder = target_table.alias("target").merge(
                source_df.alias("source"), join_condition
            )
            if when_matched_delete_condition:
                merge_builder = merge_builder.whenMatchedDelete(
                    condition=when_matched_delete_condition
                )

            merge_builder.whenMatchedUpdateAll(
                condition=when_matched_update_condition
            ).whenNotMatchedInsertAll(
                condition=when_not_matched_insert_condition
            ).execute()
        except Exception as e:
            logger.error(f"Merge operation on {full_table_name} failed: {e}")

    @staticmethod
    def vacuum(table: str, retention_hours: int, client: SparkClient):
        """Vacuum a Delta table.

        Vacuum remove unused files (files not managed by Delta + files
        that are not in the latest state).
        After vacuum it's impossible to time travel to versions
        older than the `retention` time.
        Default retention is 7 days. Lower retentions will be warned,
        unless it's set to false.
        Set spark.databricks.delta.retentionDurationCheck.enabled
        to false for low retentions.
        https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html
        """

        command = f"VACUUM {table} RETAIN {retention_hours} HOURS"
        logger.info(f"Running vacuum with command {command}")
        client.conn.sql(command)
        logger.info(f"Vacuum successful for table {table}")

    @staticmethod
    def optimize(
        client: SparkClient,
        table: str = None,
        z_order: list = None,
        date_column: str = "timestamp",
        from_date: str = None,
        auto_compact: bool = False,
        optimize_write: bool = False,
    ):
        """Optimize a Delta table.

        For auto-compaction and optimize write DBR >= 14.3 LTS
        and Delta >= 3.1.0 are MANDATORY.
        For z-ordering DBR >= 13.3 LTS and Delta >= 2.0.0 are MANDATORY.
        Auto-compaction (recommended) reduces the small file problem
        (overhead due to lots of metadata).
        Z-order by columns that is commonly used in queries
        predicates and has a high cardinality.
        https://docs.delta.io/latest/optimizations-oss.html
        """

        if auto_compact:
            client.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

        if optimize_write:
            client.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

        if table:
            command = f"OPTIMIZE {table}"

            if from_date:
                command += f"WHERE {date_column} >= {from_date}"

            if z_order:
                command += f" ZORDER BY {','.join(z_order)}"

            logger.info(f"Running optimize with command {command}...")
            client.conn.sql(command)
            logger.info(f"Optimize successful for table {table}.")
