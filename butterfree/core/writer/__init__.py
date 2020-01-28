"""Holds spark data loaders for multiple destinations."""

from butterfree.core.writer.historical_feature_store_writer import (
    HistoricalFeatureStoreWriter,
)
from butterfree.core.writer.online_feature_store_writer import OnlineFeatureStoreWriter

__all__ = ["OnlineFeatureStoreWriter", "HistoricalFeatureStoreWriter"]
