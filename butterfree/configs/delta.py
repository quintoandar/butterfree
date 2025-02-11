from dataclasses import dataclass
from typing import List, Optional


@dataclass
class DeltaConfig:
    """Configuration for Delta merge operations.

    Args:
        database: Target database name
        table: Target table name
        merge_on: List of columns to use as merge keys
        deduplicate: Whether to deduplicate before merge
        when_not_matched_insert_condition: Optional condition for inserts
        when_matched_update_condition: Optional condition for updates
        when_matched_delete_condition: Optional condition for deletes
    """

    database: str
    table: str
    merge_on: List[str]
    deduplicate: bool = False
    when_not_matched_insert: Optional[str] = None
    when_matched_update: Optional[str] = None
    when_matched_delete: Optional[str] = None
