"""Migrations' Constants."""
from butterfree.constants import columns

PARTITION_BY = [
    {"column_name": columns.PARTITION_YEAR, "type": "INT"},
    {"column_name": columns.PARTITION_MONTH, "type": "INT"},
    {"column_name": columns.PARTITION_DAY, "type": "INT"},
]
