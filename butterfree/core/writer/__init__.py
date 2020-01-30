"""Holds spark data loaders for multiple destinations."""

from butterfree.core.writer.historical_feature_store_writer import (
    HistoricalFeatureStoreWriter,
)
from butterfree.core.writer.online_feature_store_writer import OnlineFeatureStoreWriter
from butterfree.core.writer.sink import Sink
from butterfree.core.writer.writer import Writer

__all__ = ["Writer", "OnlineFeatureStoreWriter", "HistoricalFeatureStoreWriter", "Sink"]
